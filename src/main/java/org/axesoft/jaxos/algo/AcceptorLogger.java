package org.axesoft.jaxos.algo;


public interface AcceptorLogger {

    void savePromise(int squadId, long instanceId, int proposal, Event.BallotValue value);

    /**
     * @param squadId
     * @return not null, {@link Instance#emptyOf(int)} if no such instance
     */
    Instance loadLastPromise(int squadId);

    Instance loadPromise(int squadId, long instanceId);

    void saveCheckPoint(CheckPoint checkPoint);

    CheckPoint loadLastCheckPoint(int squadId);

    void sync();

    void close();

    void printMetrics(long currentMillis);
}
