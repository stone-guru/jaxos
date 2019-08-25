package org.jaxos.network;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public interface RequestDispatcher {
    /**
     * The entry point of all network request
     *
     * @param buff not null
     * @return the response message, null means not response
     */
    byte[] process(byte[] buff);
}
