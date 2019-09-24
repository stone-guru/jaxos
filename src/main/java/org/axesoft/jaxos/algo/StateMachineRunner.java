package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.google.common.base.Preconditions.checkNotNull;

public class StateMachineRunner implements Learner {
    private static Logger logger = LoggerFactory.getLogger(StateMachineRunner.class);

    private StateMachine machine;
    private volatile LastChosen lastChosen;

    public StateMachineRunner(StateMachine machine) {
        this.machine = checkNotNull(machine);
    }

    public StateMachine machine(){
        return this.machine;
    }

    @Override
    public void learnLastChosen(int squadId, long instanceId, int proposal) {
        this.lastChosen = new LastChosen(squadId, instanceId, proposal);
        machine.learnLastChosenVersion(squadId, instanceId);
    }

    @Override
    public synchronized LastChosen lastChosen(int squadId) {
        return this.lastChosen;
    }

    @Override
    public synchronized long lastChosenInstanceId(int squadId) {
        return this.lastChosen.instanceId;
    }

    @Override
    public synchronized void learnValue(int squadId, long instanceId, int proposal, ByteString value) {
        try {
            this.lastChosen = new LastChosen(squadId, instanceId, proposal);
            machine.consume(squadId, instanceId, value);
        } catch (Exception e) {
            //FIXME hold the whole jaxos system
            throw new RuntimeException(e);
        }
    }
}
