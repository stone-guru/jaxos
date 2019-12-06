package org.axesoft.jaxos.algo;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;

/**
 * @author gaoyuan
 * @sine 2019/9/27.
 */
public class Instance implements Serializable {
    private static final Map<Integer, Instance> EMPTY_INSTANCES = initEmptyInstances();

    private static Map<Integer, Instance> initEmptyInstances() {
        ImmutableMap.Builder<Integer, Instance> builder = ImmutableMap.builder();
        for (int i = 0; i < 256; i++) {
            builder.put(i, new Instance(i, 0, 0, Event.BallotValue.EMPTY));
        }
        return builder.build();
    }

    public static Instance emptyOf(int squadId) {
        Instance i = EMPTY_INSTANCES.get(squadId);
        if (i != null) {
            return i;
        }
        else {
            throw new IllegalArgumentException("No such instance for squad " + squadId);
        }
    }

    private int squadId;
    private long instanceId;
    private int proposal;
    private Event.BallotValue value;

    public Instance(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        this.squadId = squadId;
        this.instanceId = instanceId;
        this.proposal = proposal;
        this.value = value;
    }

    public int squadId() {
        return this.squadId;
    }

    public long instanceId() {
        return this.instanceId;
    }

    public int proposal() {
        return this.proposal;
    }

    public Event.BallotValue value() {
        return this.value;
    }

    public boolean isEmpty() {
        return this.instanceId == 0;
    }

    @Override
    public String toString() {
        return "Promise{" +
                "squadId=" + squadId +
                ", instanceId=" + instanceId +
                ", proposal=" + proposal +
                ", value=" + value +
                '}';
    }
}
