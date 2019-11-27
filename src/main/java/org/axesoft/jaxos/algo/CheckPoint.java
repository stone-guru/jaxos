package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.Date;

/**
 * @author gaoyuan
 * @sine 2019/9/24.
 */
public class CheckPoint implements Serializable {
    public static CheckPoint EMPTY = new CheckPoint(0, 0, 0, ByteString.EMPTY);

    private int squadId;
    private long instanceId;
    private long timestamp;
    private ByteString content;

    public CheckPoint(int squadId, long instanceId, long timestamp, ByteString content) {
        this.squadId = squadId;
        this.instanceId = instanceId;
        this.timestamp = timestamp;
        this.content = content;
    }

    public int squadId(){
        return this.squadId;
    }

    public long instanceId(){
        return this.instanceId;
    }

    public long timestamp(){
        return this.timestamp;
    }

    public ByteString content(){
        return this.content;
    }

    public boolean isEmpty(){
        return this.content.isEmpty();
    }

    @Override
    public String toString() {
        return "CheckPoint{" +
                "squadId=" + squadId +
                ", instanceId=" + instanceId +
                ", timestamp=" + new Date(timestamp)+
                ", content=BX[" + content.size() + "]" +
                '}';
    }
}
