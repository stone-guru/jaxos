package org.axesoft.jaxos.algo;

import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.base.Velometer;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class SquadMetrics {
    public enum ProposalResult {
        SUCCESS, CONFLICT, OTHER
    }

    private Velometer proposeVelometer = new Velometer();
    private Velometer acceptVelometer = new Velometer();
    private Velometer conflictVelometer = new Velometer();
    private Velometer successVelometer = new Velometer();
    private Velometer otherVelometer = new Velometer();
    private volatile double currentProposeElapsed;
    private volatile double currentAcceptElapsed;

    public void recordAccept(long nanos){
        acceptVelometer.record(nanos);
    }

    public void recordPropose(long nanos, ProposalResult result){
        proposeVelometer.record(nanos);

        switch (result){
            case SUCCESS:
                successVelometer.record(1);
                break;
            case CONFLICT:
                conflictVelometer.record(1);
                break;
            default:
                otherVelometer.record(1);
        }
    }

    public long lastTime(){
        return proposeVelometer.lastTimestamp();
    }

    public long proposeTimes(){
        return proposeVelometer.times();
    }

    public long proposeDelta() {
        return proposeVelometer.timesDelta();
    }

    public long acceptTimes(){
        return acceptVelometer.times();
    }

    public long acceptDelta(){
        return acceptVelometer.timesDelta();
    }

    public Pair<Double, Double> compute(long timestamp){
        this.currentProposeElapsed = proposeVelometer.compute(timestamp);
        this.currentAcceptElapsed = acceptVelometer.compute(timestamp);
        this.conflictVelometer.compute(timestamp);
        this.successVelometer.compute(timestamp);
        this.otherVelometer.compute(timestamp);

        return Pair.of(this.currentProposeElapsed, this.currentAcceptElapsed);
    }

    public double totalSuccessRate(){
        return this.successVelometer.times() / (double) proposeVelometer.times();
    }

    public double successRate(){
        return successVelometer.timesDelta() / (double) proposeVelometer.timesDelta();
    }

    public double conflictRate(){
        return conflictVelometer.timesDelta() / (double) proposeVelometer.timesDelta();
    }

    public double otherRate() {
        return otherVelometer.timesDelta() / (double) proposeVelometer.timesDelta();
    }
}
