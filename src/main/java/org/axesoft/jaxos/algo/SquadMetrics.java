package org.axesoft.jaxos.algo;

import org.axesoft.jaxos.base.Velometer;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class SquadMetrics {
    private Velometer proposeVelometer = new Velometer();
    private Velometer conflictVelometer = new Velometer();
    private Velometer successVelometer = new Velometer();
    private Velometer otherVelometer = new Velometer();
    private volatile double currentProposeElapsed;

    public void recordPropose(long nanos, ProposeResult r){
        proposeVelometer.record(nanos);

        switch (r.code()){
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

    public long times(){
        return proposeVelometer.times();
    }

    public long delta() {
        return proposeVelometer.timesDelta();
    }

    public double compute(long timestamp){
        this.currentProposeElapsed = proposeVelometer.compute(timestamp);

        this.conflictVelometer.compute(timestamp);
        this.successVelometer.compute(timestamp);
        this.otherVelometer.compute(timestamp);

        return this.currentProposeElapsed;
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
