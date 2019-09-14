package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

/**
 * @author gaoyuan
 * @sine 2019/8/30.
 */
public class ValueWithProposal {
    public static final ValueWithProposal NONE = new ValueWithProposal(0, ByteString.EMPTY);

    public static ValueWithProposal of(int proposal, ByteString content){
        return new ValueWithProposal(proposal, content);
    }

    public final int ballot;
    public final ByteString content;

    public ValueWithProposal(int ballot, ByteString content) {
        this.ballot = ballot;
        this.content = content;
    }
}
