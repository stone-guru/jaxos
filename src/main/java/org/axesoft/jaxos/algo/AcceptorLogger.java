package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import java.io.Serializable;

public interface AcceptorLogger {
    class Promise implements Serializable {
        public int squadId;
        public long instanceId;
        public int proposal;
        public ByteString value;
    }

    void saveLastPromise(int squadId, long instanceId, int proposal, ByteString value);

    Promise loadLastPromise(int squadId);

    void close();
}
