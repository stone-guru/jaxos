package org.jaxos.network;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public interface RequestDispatcher {
    byte[] process(byte[] buff);
}
