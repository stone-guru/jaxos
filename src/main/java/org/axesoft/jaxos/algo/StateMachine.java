package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface StateMachine {

    long currentVersion(int squadId);

    void consume(int squadId, long instanceId, ByteString message);

    void close();

    CheckPoint makeCheckPoint(int squadId);

    void restoreFromCheckPoint(CheckPoint checkPoint);
}
