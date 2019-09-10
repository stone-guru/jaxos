package org.jaxos.algo;

import org.jaxos.JaxosConfig;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class ProposeResult {
    public enum Code {
        SUCCESS, NO_QUORUM, NOT_LEADER
    }

    public static final ProposeResult NO_QUORUM = new ProposeResult(Code.NO_QUORUM);

    public static ProposeResult success(long instanceId){
      return new ProposeResult(Code.SUCCESS, instanceId);
    }

    public static ProposeResult notLeader(JaxosConfig.Peer peer) {
        checkNotNull(peer);
        return new ProposeResult(Code.NOT_LEADER, peer);
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
