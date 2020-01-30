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
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * A paxos logger implementation based on the RocksDB and ProtoBuff message coder
 */
public class RocksDbAcceptorLogger implements AcceptorLogger {
    static {
        RocksDB.loadLibrary();
    }

    private static final Logger logger = LoggerFactory.getLogger(RocksDbAcceptorLogger.class);

    private static final int KEEP_OLD_LOG_NUM = 5000;

    private static final byte CATEGORY_SQUAD_LAST = 1;
    private static final byte CATEGORY_PROMISE = 2;
    private static final byte CATEGORY_SQUAD_CHECKPOINT = 3;
    private static final byte CATEGORY_OLDEST_INSTANCE_ID = 4;

    private static final byte[] EMPTY_KEY = new byte[]{0};
    private static final byte[] EMPTY_VALUE = new byte[]{0};

    private Duration syncInterval;
    private ProtoMessageCoder messageCoder;
    private String path;
    private int squadCount;

    private RocksDB db;

    private JaxosMetrics metrics;

    private ConcurrentMap<Integer, ColumnFamilyHandle> columnFamilyHandleMap = new ConcurrentHashMap<>();

    private static class SquadSyncTimestamp {
        public final AtomicStampedReference<Long> persistRef;
        public final AtomicStampedReference<Long> syncRef;

        public SquadSyncTimestamp(long cur) {
            this.persistRef = new AtomicStampedReference<>(cur, 0);
            this.syncRef = new AtomicStampedReference<>(cur, 0);
        }
    }

    private ConcurrentMap<Integer, SquadSyncTimestamp> squadSyncTimestampMap = new ConcurrentHashMap<>();

    public RocksDbAcceptorLogger(String path, int squadCount, Duration syncInterval, JaxosMetrics metrics) {
        this.squadCount = squadCount;
        this.path = path;
        this.messageCoder = new ProtoMessageCoder();
        this.syncInterval = syncInterval;

        this.metrics = metrics;
        tryCreateDir(path);

        DBOptions options = new DBOptions()
                .setCreateIfMissing(true);

        long cur = System.currentTimeMillis();
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(new ColumnFamilyDescriptor(("default").getBytes(Charset.forName("UTF-8"))));
        for (int i = 0; i < squadCount; i++) {
            descriptors.add(new ColumnFamilyDescriptor(("S" + i).getBytes(Charset.forName("UTF-8"))));
            squadSyncTimestampMap.put(i, new SquadSyncTimestamp(cur));
        }

        List<ColumnFamilyHandle> handlers = new ArrayList<>();
        try {
            db = RocksDB.open(options, path, descriptors, handlers);

            for (int i = 0; i < squadCount; i++) {
                ColumnFamilyDescriptor desc = new ColumnFamilyDescriptor(("S" + i).getBytes(Charset.forName("UTF-8")));
                columnFamilyHandleMap.put(i, db.createColumnFamily(desc));
            }

        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private ColumnFamilyHandle getColumnFamilyHandle(int i) {
        return columnFamilyHandleMap.computeIfAbsent(i, k -> {
            String msg = String.format("Column Family Handle for %d not exist", k);
            throw new RuntimeException(msg);
        });
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
    public void saveInstance(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        long t0 = System.nanoTime();

        byte[] data = toByteArray(new Instance(squadId, instanceId, proposal, value));

        long current = System.currentTimeMillis();
        recordSaveTimestamp(squadId, current);
        boolean shouldSync = computeShouldSync(squadId, current);

        byte[] instanceKey = keyOfInstanceId(squadId, instanceId);
        try (final WriteBatch writeBatch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions().setSync(shouldSync)) {
            ColumnFamilyHandle handle = getColumnFamilyHandle(squadId);
            writeBatch.put(handle, instanceKey, data);
            writeBatch.put(handle, keyOfSquadLast(squadId), instanceKey);
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

    private void recordSaveTimestamp(int squadId, long t) {
        SquadSyncTimestamp squadStamp = squadSyncTimestampMap.get(squadId);
        for (; ; ) {
            int stamp = squadStamp.persistRef.getStamp();
            Long t0 = squadStamp.persistRef.getReference();

            if (squadStamp.persistRef.compareAndSet(t0, Math.max(t0, t), stamp, stamp + 1)) {
                return;
            }
        }
    }

    private boolean computeShouldSync(int squad, long current) {
        SquadSyncTimestamp stamp = squadSyncTimestampMap.get(squad);

        int pt = stamp.persistRef.getStamp();
        int st = stamp.syncRef.getStamp();
        Long t0 = stamp.syncRef.getReference();
        //logger.trace("PT={}, ST={}, T0={}, CUR={}", pt, st, t0, current);

        if (st >= pt) {
            return false;
        }

        if (current - t0 < syncInterval.toMillis()) {
            return false;
        }

        boolean ret = stamp.syncRef.compareAndSet(t0, current, st, pt);
        if (ret && logger.isTraceEnabled()) {
            logger.trace("should do syncRef");
        }
        return ret || syncInterval.isZero();
    }

    @Override
    public Instance loadLastInstance(int squadId) {
        long t0 = System.nanoTime();

        try {
            byte[] indexKey = keyOfSquadLast(squadId);
            return loadInstanceByIndex(indexKey, squadId);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        finally {
            this.metrics.recordLoggerLoadElapsed(System.nanoTime() - t0);
        }
    }

    private Instance loadInstanceByIndex(byte[] indexKey, int squadId) throws RocksDBException {
        byte[] idx = db.get(getColumnFamilyHandle(squadId), indexKey);
        if (idx == null) {
            return Instance.emptyOf(squadId);
        }

        byte[] bx = db.get(idx);
        if (bx == null) {
            return Instance.emptyOf(squadId);
        }

        return toEntity(bx);
    }

    @Override
    public Instance loadInstance(int squadId, long instanceId) {
        long t0 = System.nanoTime();
        try {
            byte[] key = keyOfInstanceId(squadId, instanceId);
            byte[] bx = db.get(getColumnFamilyHandle(squadId), key);

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


        try (final WriteOptions writeOptions = new WriteOptions().setSync(true)) {
            db.put(getColumnFamilyHandle(checkPoint.squadId()), writeOptions, key, data);
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        this.metrics.recordLoggerSaveCheckPointElapse(System.nanoTime() - t0);

        if (deleteOldInstances) {
            deleteLogLessEqual(checkPoint.squadId(), checkPoint.instanceId() - KEEP_OLD_LOG_NUM);
        }
    }

    private void deleteLogLessEqual(int squadId, long instanceId) {
        if (instanceId <= 0) {
            return;
        }

        long t0 = System.nanoTime();
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions opt = new WriteOptions().setSync(true)) {
            writeBatch.deleteRange(getColumnFamilyHandle(squadId),
                    keyOfInstanceId(squadId, 0),
                    keyOfInstanceId(squadId, instanceId));
            db.write(opt, writeBatch);

            db.compactRange();
        }
        catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        double millis = (System.nanoTime() - t0) / 1e+6;
        this.metrics.recordLoggerDeleteElapsedMillis((long) millis);
        logger.info("S{} delete instances before {}, elapsed {} ms", squadId, instanceId, millis);
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        byte[] key = keyOfCheckPoint(squadId);
        byte[] data;
        try {
            data = db.get(getColumnFamilyHandle(squadId), key);
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
        for(int i = 0; i < this.squadCount; i++){
            this.sync(i, force);
        }
    }

    private void sync(int squadId, boolean force){
        if (!force && !computeShouldSync(squadId, System.currentTimeMillis())) {
            return;
        }

        long t0 = System.nanoTime();

        try (final WriteOptions writeOptions = new WriteOptions().setSync(true);
             final WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.put(getColumnFamilyHandle(squadId), EMPTY_KEY, EMPTY_VALUE);
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
