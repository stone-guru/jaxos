package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import java.io.Serializable;

/**
 * @author gaoyuan
 * @sine 2019/9/27.
 */
public class InstanceValue implements Serializable {
    public int squadId;
    public long instanceId;
    public int proposal;
    public Event.BallotValue value;

    public InstanceValue() {
        this(0, 0, 0, Event.BallotValue.EMPTY);
    }

    public InstanceValue(int squadId, long instanceId, int proposal, Event.BallotValue value) {
        this.squadId = squadId;
        this.instanceId = instanceId;
        this.proposal = proposal;
        this.value = value;
    }

    public int squadId(){
        return this.squadId;
    }

    public long instanceId(){
        return this.instanceId;
    }

    public int proposal(){
        return this.proposal;
    }

    public Event.BallotValue value(){
        return this.value;
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
