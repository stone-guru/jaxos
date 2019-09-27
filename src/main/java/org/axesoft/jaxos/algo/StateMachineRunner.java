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

    public StateMachineRunner(StateMachine machine) {
        this.machine = checkNotNull(machine);
        this.lastChosenMap = new HashMap<>();
    }

    public StateMachine machine(){
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
        if(c == null){
            return new LastChosen(squadId, 0, 0);
        }
        return c;
    }

    @Override
    public synchronized long lastChosenInstanceId(int squadId) {
        return this.lastChosen(squadId).instanceId;
    }

    @Override
    public synchronized void learnValue(int squadId, long instanceId, int proposal, ByteString value) {
        try {
            this.lastChosenMap.put(squadId,new LastChosen(squadId, instanceId, proposal));
            this.machine.consume(squadId, instanceId, value);
        } catch (Exception e) {
            //FIXME let outer class know and hold the whole jaxos system
            throw new RuntimeException(e);
        }
    }
}
