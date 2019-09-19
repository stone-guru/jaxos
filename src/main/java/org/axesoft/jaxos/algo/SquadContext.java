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
    private static final Logger logger = LoggerFactory.getLogger(SquadContext.class);

    public static class SuccessRequestRecord {
        private final int serverId;
        private final long timestampMillis;
        private final int proposal;

        public SuccessRequestRecord(int serverId, long timestampMillis, int proposal) {
            this.serverId = serverId;
            this.timestampMillis = timestampMillis;
            this.proposal = proposal;
        }

        public int serverId() {
            return this.serverId;
        }

        public int proposal(){
            return this.proposal;
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

    private JaxosMetrics jaxosMetrics = new JaxosMetrics();
    private JaxosSettings config;
    private int squadId;
    private volatile SuccessRequestRecord lastSuccessRequestRecord = new SuccessRequestRecord(-1, 0, 0);

    public SquadContext(int squadId, JaxosSettings config) {
        this.config = config;
        this.squadId = squadId;
    }

    public JaxosMetrics jaxosMetrics() {
        return this.jaxosMetrics;
    }

    public void setPrepareSuccessRecord(int serverId, int proposal){
        this.lastSuccessRequestRecord = new SuccessRequestRecord(serverId, System.currentTimeMillis(), proposal);
    }

    public SuccessRequestRecord lastSuccessPrepare() {
        return this.lastSuccessRequestRecord;
    }

    public boolean isOtherLeaderActive() {
        return lastSuccessRequestRecord.serverId() != -1 && lastSuccessRequestRecord.serverId() != config.serverId()
                && !leaderLeaseExpired(lastSuccessRequestRecord.timestampMillis());
    }

    public boolean isLeader() {
        return (lastSuccessRequestRecord.serverId() == config.serverId()) && !leaderLeaseExpired(lastSuccessRequestRecord.timestampMillis());
    }

    public int squadId() {
        return this.squadId;
    }

    private boolean leaderLeaseExpired(long timestampMillis) {
        return (System.currentTimeMillis() - timestampMillis) / 1000.0 > config.leaderLeaseSeconds();
    }
}
