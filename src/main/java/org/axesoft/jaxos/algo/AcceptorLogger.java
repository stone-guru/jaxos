package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import java.io.Serializable;

public interface AcceptorLogger {
    class Promise implements Serializable {
        public int squadId;
        public long instanceId;
        public int proposal;
        public ByteString value;

        @Override
        public String toString() {
            return "Promise{" +
                    "squadId=" + squadId +
                    ", instanceId=" + instanceId +
                    ", proposal=" + proposal +
                    ", value=B[" + value.size() + "]" +
                    '}';
        }
    }

    void savePromise(int squadId, long instanceId, int proposal, ByteString value);

    Promise loadLastPromise(int squadId);

    Promise loadPromise(int squadId, long instanceId);

    void close();
}
