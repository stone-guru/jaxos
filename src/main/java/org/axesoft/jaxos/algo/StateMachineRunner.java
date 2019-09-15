package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.google.common.base.Preconditions.checkNotNull;

public class StateMachineRunner implements Learner {
    private static Logger logger = LoggerFactory.getLogger(StateMachineRunner.class);

    private StateMachine machine;
    private long lastChosenInstanceId;

    public StateMachineRunner(StateMachine machine) {
        this.machine = checkNotNull(machine);
    }

    @Override
    public synchronized void learnLastChosenInstanceId(long instanceId) {
        if(instanceId > lastChosenInstanceId){
            lastChosenInstanceId = instanceId;
        }
    }

    @Override
    public synchronized long lastChosenInstanceId() {
        return this.lastChosenInstanceId;
    }

    @Override
    public synchronized void learnValue(long instanceId, int proposal, ByteString value) {
        try {
            if (this.lastChosenInstanceId < instanceId) {
                this.lastChosenInstanceId = instanceId;
                machine.consume(instanceId, value);
            }
            else {
                logger.error("New instance id {} less than last chosen instance id {}", instanceId, this.lastChosenInstanceId);
            }
        } catch (Exception e) {
            //FIXME hold the whole jaxos system
            throw new RuntimeException(e);
        }
    }
}
