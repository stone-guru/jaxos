package org.jaxos.network;

import org.jaxos.algo.Event;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public interface RequestSender {
    void broadcast(Event msg);
}
