package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

/**
 * @author gaoyuan
 * @sine 2019/9/9.
 */
public interface Learner {
    class LastChosen {
        final int squadId;
        final long instanceId;
        final int proposal;

        public LastChosen(int squadId, long instanceId, int proposal) {
            this.squadId = squadId;
            this.instanceId = instanceId;
            this.proposal = proposal;
        }
    }

    void learnLastChosen(int squadId, long instanceId, int proposal);

    LastChosen lastChosen(int squadId);

    long lastChosenInstanceId(int squadId);

    void learnValue(int squadId, long instanceId, int proposal, ByteString value);
}
