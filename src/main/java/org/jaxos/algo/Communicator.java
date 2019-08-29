package org.jaxos.algo;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public interface Communicator  {
    void broadcast(Event msg);
    void close();
}
