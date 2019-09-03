package org.jaxos.algo;

import com.google.protobuf.ByteString;

/**
 * @author gaoyuan
 * @sine 2019/8/30.
 */
public class AcceptedValue {
    public static final AcceptedValue NONE = new AcceptedValue(0, ByteString.EMPTY);

    public final int ballot;
    public final ByteString content;

    public AcceptedValue(int ballot, ByteString content) {
        this.ballot = ballot;
        this.content = content;
    }
}
