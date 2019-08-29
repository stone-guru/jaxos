package org.jaxos.network;

import org.jaxos.algo.Communicator;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public interface CommunicatorFactory {
    Communicator createCommunicator();
}
