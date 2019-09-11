package org.jaxos.algo;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.sleepycat.je.*;
import org.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/2.
 */
public class InstanceContext implements Learner{

    public static class RequestRecord {
        private final int serverId;
        private final long timestampMillis;

        public RequestRecord(int serverId, long timestampMillis) {
            this.serverId = serverId;
            this.timestampMillis = timestampMillis;
        }

        public int serverId() {
            return this.serverId;
        }

        public long timestampMillis() {
            return this.timestampMillis;
        }

        @Override
        public String toString() {
            return "RequestInfo{" +
                    "serverId=" + serverId +
                    ", timestampMillis=" + new Date(timestampMillis) +
                    '}';
        }
    }
    private static final Logger logger = LoggerFactory.getLogger(InstanceContext.class);

    private AtomicLong lastInstanceId = new AtomicLong(0);
    private ConcurrentMap<Long, ByteString> histValues = new ConcurrentHashMap<>();
    private volatile RequestRecord lastRequestRecord = new RequestRecord(-1, 0);
    private JaxosMetrics jaxosMetrics = new JaxosMetrics();
    private JaxosConfig config;
    private Database db;
    private Environment dbEnv;

    public InstanceContext(JaxosConfig config) {
        this.config = config;
        initDb("./log/db" + config.serverId());
    }

    public JaxosMetrics jaxosMetrics() {
        return this.jaxosMetrics;
    }

    public long lastInstanceId() {
        return lastInstanceId.get();
    }

    public ByteString valueOf(long instanceId) {
        return histValues.getOrDefault(instanceId, ByteString.EMPTY);
    }

    @Override
    public void learnValue(long instanceId, ByteString value) {
        if (this.histValues.put(instanceId, value) == null) {
            for (; ; ) {
                long i0 = this.lastInstanceId.get();
                if (instanceId > i0) {
                    if (this.lastInstanceId.compareAndSet(i0, instanceId)) {
                        break;
                    }
                }
            }

            saveLog(instanceId, value);
        }
    }

    private void saveLog(long instanceId, ByteString value){
        DatabaseEntry k = new DatabaseEntry(Longs.toByteArray(instanceId));
        DatabaseEntry v = new DatabaseEntry(value.toByteArray());
        db.put(null, k, v);
    }

    public void recordLastRequest(int serverId, long timeStampMillis) {
        this.lastRequestRecord = new RequestRecord(serverId, timeStampMillis);
    }

    public RequestRecord getLastRequestRecord() {
        return this.lastRequestRecord;
    }

    public boolean isOtherLeaderActive() {
        return lastRequestRecord.serverId() != -1 && lastRequestRecord.serverId() != config.serverId()
                && !leaderLeaseExpired(lastRequestRecord.timestampMillis());
    }

    public boolean isLeader() {
        return (lastRequestRecord.serverId() == config.serverId()) && !leaderLeaseExpired(lastRequestRecord.timestampMillis());
    }

    private boolean leaderLeaseExpired(long timestampMillis) {
        return (System.currentTimeMillis() - timestampMillis) / 1000.0 > config.leaderLeaseSeconds();
    }

    private void initDb(String path){
        EnvironmentConfig config = new EnvironmentConfig();
        config.setAllowCreate(true);
        this.dbEnv = new Environment(new File(path), config);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        this.db = this.dbEnv.openDatabase(null, "jaxosdb" + this.config.serverId(), dbConfig);
    }
}
