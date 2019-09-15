package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface StateMachine {
    void consume(long instanceId, ByteString message);
}
