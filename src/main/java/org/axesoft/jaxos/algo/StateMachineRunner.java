package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class StateMachineRunner implements Learner {
    private static Logger logger = LoggerFactory.getLogger(StateMachineRunner.class);

    private StateMachine machine;
    private Map<Integer, LastChosen> lastChosenMap;
    private Map<Long, InstanceValue> cachedBallots;

    public StateMachineRunner(StateMachine machine) {
        this.machine = checkNotNull(machine);
        this.lastChosenMap = new HashMap<>();
        this.cachedBallots = new HashMap<>();
    }

    public StateMachine machine() {
        return this.machine;
    }

    @Override
    public synchronized void learnLastChosen(int squadId, long instanceId, int proposal) {
        this.lastChosenMap.put(squadId, new LastChosen(squadId, instanceId, proposal));
        machine.learnLastChosenVersion(squadId, instanceId);
    }

    @Override
    public synchronized LastChosen lastChosen(int squadId) {
        LastChosen c = this.lastChosenMap.get(squadId);
        if (c == null) {
            return new LastChosen(squadId, 0, 0);
        }
        return c;
    }

    @Override
    public synchronized long lastChosenInstanceId(int squadId) {
        return this.lastChosen(squadId).instanceId;
    }

    @Override
    public synchronized boolean learnValue(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        if(instanceId != this.lastChosen(squadId).instanceId + 1){
            logger.warn("Learning ignore given instance {}, mine is {}", instanceId, this.lastChosen(squadId).instanceId);
            return false;
        }
        innerLearn(squadId, instanceId, proposal, value);
        long i = instanceId + 1;
        InstanceValue b = null;
        do {
            b = this.cachedBallots.remove(i);
            if (b != null) {
                innerLearn(b.squadId, b.instanceId, b.proposal, b.value);
                i++;
            }
        } while (b != null);
        return true;
    }

    private void innerLearn(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        try {
            this.lastChosenMap.put(squadId, new LastChosen(squadId, instanceId, proposal));
            if(value.type() == Event.ValueType.APPLICATION) {
                this.machine.consume(squadId, instanceId, value.content());
            } else {
                this.machine.consume(squadId, instanceId, ByteString.EMPTY);
            }
        }
        catch (Exception e) {
            //FIXME let outer class know and hold the whole jaxos system
            throw e;
        }
    }

    @Override
    public synchronized void cacheChosenValue(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        this.cachedBallots.put(instanceId, new InstanceValue(squadId, instanceId, proposal, value));
    }
}
