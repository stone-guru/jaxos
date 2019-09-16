package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface StateMachine {
    void learnLastChosenVersion(long instanceId);
    long currentVersion();
    void consume(long instanceId, ByteString message);
}
