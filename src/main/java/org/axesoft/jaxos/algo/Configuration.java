package org.axesoft.jaxos.algo;

/**
 * @author gaoyuan
 * @sine 2019/9/28.
 */
public interface Configuration {
    Communicator getCommunicator();

    AcceptorLogger getLogger();

    EventWorkerPool getWorkerPool();

    default EventTimer getEventTimer() {
        return getWorkerPool();
    }
}
