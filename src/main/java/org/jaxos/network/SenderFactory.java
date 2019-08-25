package org.jaxos.network;

import org.jaxos.JaxosConfig;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public interface SenderFactory {
    RequestSender createSender();
}
