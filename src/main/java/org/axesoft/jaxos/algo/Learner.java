package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

/**
 * @author gaoyuan
 * @sine 2019/9/9.
 */
public interface Learner {
    void learnLastChosenInstanceId(long instanceId);

    long lastChosenInstanceId();
    void learnValue(long instanceId, int proposal, ByteString value);
}
