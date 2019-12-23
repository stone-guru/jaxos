package org.axesoft.jaxos.logger;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.algo.CheckPoint;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.Instance;
import org.axesoft.jaxos.base.Velometer;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.axesoft.jaxos.network.protobuff.ProtoMessageCoder;
import org.iq80.leveldb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicStampedReference;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * A paxos logger implementation based on the LevelDB and ProtoBuff message coder
 */
public class LevelDbAcceptorLogger implements AcceptorLogger {
    private static final Logger logger = LoggerFactory.getLogger(LevelDbAcceptorLogger.class);

    private static final int KEEP_OLD_LOG_NUM = 2000;

    private static final byte CATEGORY_SQUAD_LAST = 1;
    private static final byte CATEGORY_PROMISE = 2;
    private static final byte CATEGORY_SQUAD_CHECKPOINT = 3;
    private static final byte CATEGORY_OLDEST_INSTANCE_ID = 4;

    private DB db;
    private String path;
    private ProtoMessageCoder messageCoder;
    private AtomicStampedReference<Long> persistTimestamp;
    private AtomicStampedReference<Long> syncTimestamp;
    private Duration syncInterval;

    private Metrics metrics;

    public LevelDbAcceptorLogger(String path, Duration syncInterval) {
        this.path = path;

        tryCreateDir(path);

        Options options = new Options()
                .createIfMissing(true)
                .compressionType(CompressionType.SNAPPY)
                .cacheSize(32 * 1048576);//32M

        try {
            db = factory.open(new File(path), options);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.messageCoder = new ProtoMessageCoder();
        this.syncInterval = syncInterval;

        long cur = System.currentTimeMillis();
        persistTimestamp = new AtomicStampedReference<>(cur, 0);
        syncTimestamp = new AtomicStampedReference<>(cur, 0);

        this.metrics = new Metrics(cur);
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
        WriteOptions writeOptions = new WriteOptions().sync(shouldSync);

        WriteBatch writeBatch = db.createWriteBatch();
        byte[] promiseKey = keyOfInstanceId(squadId, instanceId);
        writeBatch.put(promiseKey, data);
        writeBatch.put(keyOfSquadLast(squadId), promiseKey);
        db.write(writeBatch, writeOptions);

        long duration = System.nanoTime() - t0;
        this.metrics.saveVelometer.record(duration);
        if (shouldSync) {
            this.metrics.syncVelometer.record(duration);
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
        finally {
            this.metrics.loadVelometer.record(System.nanoTime() - t0);
        }
    }

    @Override
    public void saveCheckPoint(CheckPoint checkPoint) {
        byte[] key = keyOfCheckPoint(checkPoint.squadId());
        byte[] data = toByteArray(checkPoint);

        WriteOptions writeOptions = new WriteOptions().sync(true);
        db.put(key, data, writeOptions);

        deleteLogLessEqual(checkPoint.squadId(), checkPoint.instanceId());
    }

    private void deleteLogLessEqual(int squadId, long instanceId){
        if(instanceId <= 0){
            return;
        }

        long t0 = System.nanoTime();

        long idLow = 0;
        byte[] data = db.get(keyOfOldestInstanceId(squadId));
        if(data != null){
            idLow = Longs.fromByteArray(data);
        }

        if(instanceId - idLow < KEEP_OLD_LOG_NUM){
            return;
        }

        long idHigh = instanceId - KEEP_OLD_LOG_NUM;
        WriteOptions writeOptions = new WriteOptions().sync(false);
        for(long id = idLow + 1; id <= idHigh; id++){
            db.delete(keyOfInstanceId(squadId, id), writeOptions);
        }

        WriteBatch writeBatch = db.createWriteBatch();
        writeBatch.put(keyOfOldestInstanceId(squadId), Longs.toByteArray(idHigh));
        db.write(writeBatch, new WriteOptions().sync(true));

        db.compactRange(null, null);

        double millis = (System.nanoTime() - t0)/1e+6;

        logger.info("S{} delete instances from {} to {}, elapsed {} ms", squadId, idLow + 1, idHigh, millis);
    }

    @Override
    public CheckPoint loadLastCheckPoint(int squadId) {
        byte[] key = keyOfCheckPoint(squadId);
        byte[] data = db.get(key);

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

        WriteOptions writeOptions = new WriteOptions().sync(true);
        WriteBatch writeBatch = db.createWriteBatch();
        db.write(writeBatch, writeOptions);

        this.metrics.syncVelometer.record(System.nanoTime() - t0);
    }

    @Override
    public void close() {
        try {
            this.db.close();
        }
        catch (IOException e) {
            logger.error("Error when close db in " + this.path);
        }
    }

    @Override
    public void printMetrics(long currentMillis) {
        this.metrics.compute(currentMillis);
        String msg = String.format("ST=%d, SE=%.2f, LT=%d, LE=%.2f, CT=%d, CT=%.2f",
                this.metrics.saveTimes, this.metrics.saveElapsed,
                this.metrics.loadTimes, this.metrics.loadElapsed,
                this.metrics.syncTimes, this.metrics.syncElapsed);
        logger.info(msg);
    }

    public byte[] toByteArray(CheckPoint checkPoint) {
        PaxosMessage.CheckPoint c = this.messageCoder.encodeCheckPoint(checkPoint);
        return c.toByteArray();
    }

    public CheckPoint toCheckPoint(byte[] bytes) {
        try {
            PaxosMessage.CheckPoint checkPoint = PaxosMessage.CheckPoint.parseFrom(bytes);
            return this.messageCoder.decodeCheckPoint(checkPoint);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }

    public Instance toEntity(byte[] bytes) {
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


    public byte[] toByteArray(Instance v) {
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

    private byte[] keyOfOldestInstanceId(int squadId){
        byte[] key = new byte[5];
        key[0] = CATEGORY_OLDEST_INSTANCE_ID;
        System.arraycopy(Ints.toByteArray(squadId), 0, key, 1, 4);
        return key;
    }

    public static class Metrics {
        public final Velometer saveVelometer;
        public final Velometer loadVelometer;
        public final Velometer syncVelometer;

        private long saveTimes = 0;
        private double saveElapsed = 0;
        private long loadTimes = 0;
        private double loadElapsed = 0;
        private long syncTimes = 0;
        private double syncElapsed = 0;

        public Metrics(long current) {
            this.saveVelometer = new Velometer(current);
            this.loadVelometer = new Velometer(current);
            this.syncVelometer = new Velometer(current);
        }

        public void compute(long current) {
            saveTimes = this.saveVelometer.timesDelta();
            saveElapsed = this.saveVelometer.compute(current);

            loadTimes = this.loadVelometer.timesDelta();
            loadElapsed = this.loadVelometer.compute(current);

            syncTimes = this.syncVelometer.timesDelta();
            syncElapsed = this.syncVelometer.compute(current);
        }
    }
}
