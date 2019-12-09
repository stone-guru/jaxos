package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.Date;

/**
 * @author gaoyuan
 * @sine 2019/9/24.
 */
public class CheckPoint implements Serializable {
    public static CheckPoint EMPTY = new CheckPoint(0, 0, 0, ByteString.EMPTY, Instance.emptyOf(0));

    private int squadId;
    private long version;
    private long timestamp;
    private ByteString content;
    private Instance lastInstance;

    public CheckPoint(int squadId, long instanceId, long timestamp, ByteString content, Instance lastInstance) {
        this.squadId = squadId;
        this.version = instanceId;
        this.timestamp = timestamp;
        this.content = content;
        this.lastInstance = lastInstance;
    }

    public int squadId(){
        return this.squadId;
    }

    public long instanceId(){
        return this.version;
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

    public Instance lastInstance(){
        return this.lastInstance;
    }

    @Override
    public String toString() {
        return "CheckPoint{" +
                "squadId=" + squadId +
                ", version=" + version +
                ", timestamp=" + new Date(timestamp)+
                ", content=BX[" + content.size() + "]" +
                ", lastInstance=" + this.lastInstance +
                '}';
    }
}
