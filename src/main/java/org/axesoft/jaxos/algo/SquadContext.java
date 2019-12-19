package org.axesoft.jaxos.algo;

import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

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

    private SquadMetrics jaxosMetrics = new SquadMetrics();
    private JaxosSettings config;
    private int squadId;

    private int proposerId = 0;
    private long chosenInstanceId = 0;
    private long chosenBallotId = 0;
    private int chosenProposal = 0;
    private long chosenTimestamp = 0;

    public SquadContext(int squadId, JaxosSettings config) {
        this.config = config;
        this.squadId = squadId;
    }

    public void recordChosenInfo(int proposerId, long chosenInstanceId, long chosenBallotId, int proposal){
        this.proposerId = proposerId;
        this.chosenInstanceId = chosenInstanceId;
        this.chosenBallotId = chosenBallotId;
        this.chosenProposal = proposal;
        this.chosenTimestamp = System.currentTimeMillis();

        if(logger.isTraceEnabled()){
            logger.trace("Record chosen info proposer {}, instance {}, ballotId {} at {}",
                    proposerId, chosenInstanceId, chosenBallotId, new Date(chosenTimestamp));
        }
    }

    public SquadMetrics metrics() {
        return this.jaxosMetrics;
    }

    public long chosenTimestamp(){
        return this.chosenTimestamp;
    }

    public boolean isOtherLeaderActive() {
        return proposerId > 0 && proposerId != config.serverId()
                && !leaderLeaseExpired(chosenTimestamp);
    }

    public boolean isLeader() {
        return (this.proposerId == config.serverId()) && !leaderLeaseExpired(this.chosenTimestamp);
    }

    public int squadId() {
        return this.squadId;
    }

    private boolean leaderLeaseExpired(long timestampMillis) {
        return (System.currentTimeMillis() - timestampMillis) / 1000.0 > config.leaderLeaseSeconds();
    }

    public long chosenInstanceId() {
        return this.chosenInstanceId;
    }

    public int lastProposer(){
        return this.proposerId;
    }

    public int chosenProposal(){
        return this.chosenProposal;
    }

    public Event.ChosenInfo getLastChosenInfo(){
        return new Event.ChosenInfo(chosenInstanceId, chosenBallotId, System.currentTimeMillis() - chosenTimestamp);
    }
}
