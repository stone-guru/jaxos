package org.axesoft.jaxos.logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * A paxos logger implementation based on the RocksDB and ProtoBuff message coder
 */
public class RocksDbAcceptorLogger implements AcceptorLogger {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger logger = LoggerFactory.getLogger(RocksDbAcceptorLogger.class);

    private static final int KEEP_OLD_LOG_NUM = 2000;

    private static final byte CATEGORY_SQUAD_LAST = 1;
    private static final byte CATEGORY_PROMISE = 2;
    private static final byte CATEGORY_SQUAD_CHECKPOINT = 3;
    private static final byte CATEGORY_OLDEST_INSTANCE_ID = 4;

    private RocksDB db;
    private String path;
    private ProtoMessageCoder messageCoder;
    private AtomicStampedReference<Long> persistTimestamp;
    private AtomicStampedReference<Long> syncTimestamp;
    private Duration syncInterval;

    private JaxosMetrics metrics;

    public RocksDbAcceptorLogger(String path, Duration syncInterval, JaxosMetrics metrics) {
        this.path = path;

        tryCreateDir(path);

        Options options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.NO_COMPRESSION);

        try {
            db = RocksDB.open(options, path);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        this.messageCoder = new ProtoMessageCoder();
        this.syncInterval = syncInterval;

        long cur = System.currentTimeMillis();
        persistTimestamp = new AtomicStampedReference<>(cur, 0);
        syncTimestamp = new AtomicStampedReference<>(cur, 0);

        this.metrics = metrics;
    }

    private void tryCreateDir(String path) {
        File f = new File(path);
        if (f.exists()) {
            if (!f.isDirectory()) {
                throw new RuntimeException(path + " is not a directory");
            }
        }
        else {
            if (!f.mkdir()) {
                throw new RuntimeException("can not mkdir at " + path);
            }
        }
    }

    @Override
    public void savePromise(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        long t0 = System.nanoTime();

        byte[] data = toByteArray(new Instance(squadId, instanceId, proposal, value));

        long current = System.currentTimeMillis();
        recordSaveTimestamp(current);
        boolean shouldSync = computeShouldSync(current);
        WriteOptions writeOptions = new WriteOptions().setSync(shouldSync);

        WriteBatch writeBatch = new WriteBatch();
        byte[] promiseKey = keyOfInstanceId(squadId, instanceId);
        try {
            writeBatch.put(promiseKey, data);
            writeBatch.put(keyOfSquadLast(squadId), promiseKey);
            db.write(writeOptions, writeBatch);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        long duration = System.nanoTime() - t0;
        this.metrics.recordLoggerSaveElapsed(duration);
        if (shouldSync) {
            this.metrics.recordLoggerSyncElapsed(duration);
        }
    }

    private void recordSaveTimestamp(long t) {
        for (; ; ) {
            int stamp = persistTimestamp.getStamp();
            Long t0 = persistTimestamp.getReference();

            if (persistTimestamp.compareAndSet(t0, Math.max(t0, t), stamp, stamp + 1)) {
                return;
            }
        }
    }

    private boolean computeShouldSync(long current) {
        int pt = persistTimestamp.getStamp();
        int st = syncTimestamp.getStamp();
        Long t0 = syncTimestamp.getReference();
        //logger.trace("PT={}, ST={}, T0={}, CUR={}", pt, st, t0, current);

        if (st >= pt) {
            return false;
        }

        if (current - t0 < syncInterval.toMillis()) {
            return false;
        }

        boolean ret = syncTimestamp.compareAndSet(t0, current, st, pt);
        if (ret && logger.isTraceEnabled()) {
            logger.trace("should do sync");
        }
        return ret || syncInterval.isZero();
    }

    @Override
    public Instance loadLastPromise(int squadId) {
        long t0 = System.nanoTime();

        try {
            byte[] last = keyOfSquadLast(squadId);
            byte[] idx = db.get(last);
            if (idx == null) {
                return Instance.emptyOf(squadId);
            }

            byte[] bx = db.get(idx);
            if (bx == null) {
                return Instance.emptyOf(squadId);
            }

            return toEntity(bx);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        finally {
            this.metrics.recordLoggerLoadElapsed(System.nanoTime() - t0);
        }
    }

    @Override
    public Instance loadPromise(int squadId, long instanceId) {
        long t0 = System.nanoTime();
        try {
            byte[] key = keyOfInstanceId(squadId, instanceId);
            byte[] bx = db.get(key);

            if (bx != null) {
                return toEntity(bx);
            }
            return Instance.emptyOf(squadId);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        finally {
            this.metrics.recordLoggerLoadElapsed(System.nanoTime() - t0);
        }
    }

    @Override
    public void saveCheckPoint(CheckPoint checkPoint, boolean deleteOldInstances) {
        long t0 = System.nanoTime();
        byte[] key = keyOfCheckPoint(checkPoint.squadId());
        byte[] data = toByteArray(checkPoint);

        WriteOptions writeOptions = new WriteOptions().setSync(true);
        try {
            db.put(writeOptions, key, data);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        this.metrics.recordLoggerSaveCheckPointElapse(System.nanoTime() - t0);

        if (deleteOldInstances) {
            deleteLogLessEqual(checkPoint.squadId(), checkPoint.instanceId());
        }
    }

    private void deleteLogLessEqual(int squadId, long instanceId) {
        if (instanceId <= 0) {
            return;
        }

        long t0 = System.nanoTime();

        long idLow = 0;
        long idHigh = 0;
        try {
            byte[] data = db.get(keyOfOldestInstanceId(squadId));
            if (data != null) {
                idLow = Longs.fromByteArray(data);
            }

            if (instanceId - idLow < KEEP_OLD_LOG_NUM) {
                return;
            }

            idHigh = instanceId - KEEP_OLD_LOG_NUM;
            WriteOptions writeOptions = new WriteOptions().setSync(false);
            for (long id = idLow + 1; id <= idHigh; id++) {
                db.delete(writeOptions, keyOfInstanceId(squadId, id));
            }

            WriteBatch writeBatch = new WriteBatch();
            writeBatch.put(keyOfOldestInstanceId(squadId), Longs.toByteArray(idHigh));
            db.write(new WriteOptions().setSync(true), writeBatch);

            db.compactRange(null, null);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        double millis = (System.nanoTime() - t0) / 1e+6;
        this.metrics.recordLoggerDeleteElapsedMillis((long) millis);
        logger.info("S{} delete instances from {} to {}, elapsed {} ms", squadId, idLow + 1, idHigh, millis);
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        byte[] key = keyOfCheckPoint(squadId);
        byte[] data ;
        try {
            data = db.get(key);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        if (data == null) {
            return CheckPoint.EMPTY;
        }
        return toCheckPoint(data);
    }

    @Override
    public void sync(boolean force) {
        if (!force && !computeShouldSync(System.currentTimeMillis())) {
            return;
        }

        long t0 = System.nanoTime();

        WriteOptions writeOptions = new WriteOptions().setSync(true);
        WriteBatch writeBatch = new WriteBatch();
        try {
            db.write(writeOptions, writeBatch);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        this.metrics.recordLoggerSyncElapsed(System.nanoTime() - t0);
    }

    @Override
    public void close() {
        try {
            sync(true);
            this.db.closeE();
        }
        catch (RocksDBException e) {
            logger.error("Error when close db in " + this.path);
        }
    }

    private byte[] toByteArray(CheckPoint checkPoint) {
        PaxosMessage.CheckPoint c = this.messageCoder.encodeCheckPoint(checkPoint);
        return c.toByteArray();
    }

    private CheckPoint toCheckPoint(byte[] bytes) {
        try {
            PaxosMessage.CheckPoint checkPoint = PaxosMessage.CheckPoint.parseFrom(bytes);
            return this.messageCoder.decodeCheckPoint(checkPoint);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    private Instance toEntity(byte[] bytes) {
        PaxosMessage.Instance i;
        try {
            i = PaxosMessage.Instance.parseFrom(bytes);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        Event.BallotValue v = messageCoder.decodeValue(i.getValue());
        return new Instance(i.getSquadId(), i.getInstanceId(), i.getProposal(), v);
    }


    private byte[] toByteArray(Instance v) {
        return PaxosMessage.Instance.newBuilder()
                .setSquadId(v.squadId())
                .setInstanceId(v.id())
                .setProposal(v.proposal())
                .setValue(messageCoder.encodeValue(v.value()))
                .build()
                .toByteArray();
    }

    private byte[] keyOfInstanceId(int squadId, long i) {
        byte[] key = new byte[13];
        key[0] = CATEGORY_PROMISE;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        System.arraycopy(Longs.toByteArray(i), 0, key, 5, 8);
        return key;
    }

    private byte[] keyOfSquadLast(int squadId) {
        byte[] key = new byte[5];
        key[0] = CATEGORY_SQUAD_LAST;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }

    private byte[] keyOfCheckPoint(int squadId) {
        byte[] key = new byte[5];
        key[0] = CATEGORY_SQUAD_CHECKPOINT;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }

    private byte[] keyOfOldestInstanceId(int squadId) {
        byte[] key = new byte[5];
        key[0] = CATEGORY_OLDEST_INSTANCE_ID;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }


}
