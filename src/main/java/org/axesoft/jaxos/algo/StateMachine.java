package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface StateMachine {
    void learnLastChosenVersion(int squadId, long instanceId);
    long currentVersion(int squadId);
    void consume(int squadId, long instanceId, ByteString message);
    void close();
}
