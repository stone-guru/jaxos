package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.network.protobuff.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class StateMachineRunner implements Learner {
    private static Logger logger = LoggerFactory.getLogger(StateMachineRunner.class);

    private int squadId;
    private StateMachine machine;
    private Instance lastChosen;

    public StateMachineRunner(int squadId, StateMachine machine) {
        this.squadId = squadId;
        this.machine = checkNotNull(machine);
        this.lastChosen = Instance.emptyOf(squadId);
    }

    public StateMachine machine() {
        return this.machine;
    }

    public synchronized void restoreFromCheckPoint(CheckPoint checkPoint, List<Instance> ix) {
        if (!checkPoint.isEmpty()) {
            this.machine.restoreFromCheckPoint(checkPoint.squadId(), checkPoint.instanceId(), checkPoint.content());
            this.lastChosen = checkPoint.lastInstance();
            logger.info("S{} Restore of {}", checkPoint.squadId(), checkPoint);
        }

        Iterator<Instance> it = ix.iterator();
        Instance i1, i2 = null;
        while (it.hasNext()) {
            i1 = it.next();
            if(i1.instanceId() > this.lastChosen.instanceId()){
                i2 = i1;
                break;
            }
        }

        while (i2 != null) {
            this.innerLearn(i2);
            i2 = it.hasNext()? it.next() : null;
        }
    }


    public synchronized CheckPoint makeCheckPoint(int squadId) {
        long timestamp = System.currentTimeMillis();
        Pair<ByteString, Long> p = this.machine.makeCheckPoint(squadId);
        return new CheckPoint(squadId, p.getRight(), timestamp, p.getLeft(), this.lastChosen);
    }

    @Override
    public synchronized Instance getLastChosenInstance(int squadId) {
        checkArgument(squadId == this.squadId, "given squad id %d is unequal to mine %d", squadId, this.squadId);
        return this.lastChosen;
    }

    @Override
    public synchronized void learnValue(Instance i) {
        checkArgument(i.squadId() == this.squadId, "given squad id %d is unequal to mine %d", i.squadId(), this.squadId);
        long i0 = this.lastChosen.instanceId();
        if (i.instanceId() != i0 + 1) {
            throw new IllegalStateException(String.format("Learning ignore given instance %d, mine is %d", i.instanceId(), i0));
        }
        innerLearn(i);
    }

    private void innerLearn(Instance i) {
        if (i.value().type() == Event.ValueType.APPLICATION) {
            this.machine.consume(squadId, i.instanceId(), i.value().content());
        }
        else {
            this.machine.consume(squadId, i.instanceId(), ByteString.EMPTY);
        }
        this.lastChosen = i;
    }

}
