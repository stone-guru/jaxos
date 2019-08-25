package org.jaxos.network;

import org.jaxos.algo.Event;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public interface EventEntryPoint {
    Event process(Event event);
}
