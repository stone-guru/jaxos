package org.jaxos.network;

import org.jaxos.network.protobuff.PaxosMessage;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public interface RequestDispatcher {
    /**
     * The entry point of all network request
     *
     * @param request not null
     * @return the response message, null means not response
     */
    PaxosMessage.DataGram process(PaxosMessage.DataGram request);
}
