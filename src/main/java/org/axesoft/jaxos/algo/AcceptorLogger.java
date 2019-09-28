package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface AcceptorLogger {

    void savePromise(int squadId, long instanceId, int proposal, ByteString value);

    InstanceValue loadLastPromise(int squadId);

    InstanceValue loadPromise(int squadId, long instanceId);

    void saveCheckPoint(CheckPoint checkPoint);

    CheckPoint loadLastCheckPoint(int squadId);

    void close();
}
