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

    @Override
    public synchronized void learnLastChosenInstanceId(long instanceId) {
        this.lastChosen = new LastChosen(instanceId, 0);
        machine.learnLastChosenVersion(instanceId);
    }

    @Override
    public synchronized LastChosen lastChosen() {
        return this.lastChosen;
    }

    @Override
    public synchronized long lastChosenInstanceId() {
        return this.lastChosen.instanceId;
    }

    @Override
    public synchronized void learnValue(long instanceId, int proposal, ByteString value) {
        try {
            this.lastChosen = new LastChosen(instanceId, proposal);
            machine.consume(instanceId, value);
        } catch (Exception e) {
            //FIXME hold the whole jaxos system
            throw new RuntimeException(e);
        }
    }
}
