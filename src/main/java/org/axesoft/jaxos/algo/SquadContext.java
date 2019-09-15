package org.axesoft.jaxos.algo;

import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/2.
 */
public class SquadContext {

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

    private static final Logger logger = LoggerFactory.getLogger(SquadContext.class);

    private AtomicLong lastInstanceId = new AtomicLong(0);
    private volatile RequestRecord lastRequestRecord = new RequestRecord(-1, 0);
    private JaxosMetrics jaxosMetrics = new JaxosMetrics();
    private JaxosSettings config;
    private int squadId;

    public SquadContext(int squadId, JaxosSettings config) {
        this.config = config;
        this.squadId = squadId;
    }

    public JaxosMetrics jaxosMetrics() {
        return this.jaxosMetrics;
    }

//    @Override
//    public void learnLastChosenInstanceId(long instanceId) {
//        lastInstanceId.set(instanceId);
//    }
//
//    @Override
//    public long lastChosenInstanceId() {
//        return lastInstanceId.get();
//    }
//
//    @Override
//    public void learnValue(long instanceId, int proposal, ByteString value) {
//        ValueWithProposal v = ValueWithProposal.of(proposal, value);
//
//        long i0 = this.lastInstanceId.get();
//        if (instanceId > i0) {
//            if (this.lastInstanceId.compareAndSet(i0, instanceId)) {
//                return;
//            }
//        }
//    }

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

    public int squadId() {
        return this.squadId;
    }

    private boolean leaderLeaseExpired(long timestampMillis) {
        return (System.currentTimeMillis() - timestampMillis) / 1000.0 > config.leaderLeaseSeconds();
    }
}
