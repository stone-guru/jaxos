package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class ProposeResult {
    public enum Code {
        SUCCESS, CONFLICT, NO_QUORUM, OTHER_LEADER, TIME_OUT
    }

    static final ProposeResult NO_QUORUM = new ProposeResult(Code.NO_QUORUM);

    static final ProposeResult TIME_OUT = new ProposeResult(Code.TIME_OUT);

    static final ProposeResult CONFLICT = new ProposeResult(Code.CONFLICT);

    static ProposeResult conflict(ByteString s){
        return new ProposeResult(Code.CONFLICT, s.toStringUtf8());
    }

    static ProposeResult success(long instanceId){
      return new ProposeResult(Code.SUCCESS, instanceId);
    }

    static ProposeResult otherLeader(int nodeId) {
        return new ProposeResult(Code.OTHER_LEADER, nodeId);
    }

    private Code code;
    private Object param;

    public ProposeResult(Code code) {
        this.code = code;
    }

    public ProposeResult(Code code, Object param) {
        this.code = code;
        this.param = param;
    }

    public boolean isSuccess(){
        return code == Code.SUCCESS;
    }

    public Code code(){
        return code;
    }

    public Object param(){
        return param;
    }

    @Override
    public String toString() {
        return "ProposeResult{" +
                "code=" + code +
                ", param=" + param +
                '}';
    }
}
