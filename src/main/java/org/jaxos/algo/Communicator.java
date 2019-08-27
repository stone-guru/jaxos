package org.jaxos.algo;

import org.jaxos.algo.Event;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public interface Communicator {
    void broadcast(Event msg);
}
