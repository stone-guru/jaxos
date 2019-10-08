package org.axesoft.jaxos.algo;


public interface AcceptorLogger {

    void savePromise(int squadId, long instanceId, int proposal, Event.BallotValue value);

    InstanceValue loadLastPromise(int squadId);

    InstanceValue loadPromise(int squadId, long instanceId);

    void saveCheckPoint(CheckPoint checkPoint);

    CheckPoint loadLastCheckPoint(int squadId);

    void sync();

    void close();
}
