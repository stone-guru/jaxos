package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

/**
 * @author gaoyuan
 * @sine 2019/9/9.
 */
public interface Learner {
    class LastChosen {
        final long instanceId;
        final int proposal;

        public LastChosen(long instanceId, int proposal) {
            this.instanceId = instanceId;
            this.proposal = proposal;
        }
    }

    void learnLastChosenInstanceId(long instanceId);
    LastChosen lastChosen();
    long lastChosenInstanceId();
    void learnValue(long instanceId, int proposal, ByteString value);
}
