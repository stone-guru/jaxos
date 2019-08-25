package org.jaxos.algo;

import java.util.Arrays;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public interface Event {
    enum Code {
        NOOP, HEART_BEAT, HEART_BEAT_RESPONSE, PREPARE, PREPARE_RESPONSE, ACCEPT, ACCEPT_RESPONSE
    }

    Code code();
    int senderId();
    long instanceId();

    class PrepareRequest implements Event {
        private int sender;
        private long instanceId;
        private int ballot;

        public PrepareRequest(int sender, long instanceId, int ballot) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.ballot = ballot;
        }

        @Override
        public Code code() {
            return Code.PREPARE;
        }

        @Override
        public int senderId() {
            return sender;
        }

        @Override
        public long instanceId() {
            return instanceId;
        }

        public int ballot() {
            return ballot;
        }

        public PrepareRequest setSender(int sender) {
            this.sender = sender;
            return this;
        }

        public PrepareRequest setInstanceId(long instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public PrepareRequest setBallot(int ballot) {
            this.ballot = ballot;
            return this;
        }

        @Override
        public String toString() {
            return "PrepareRequest{" +
                    "sender=" + sender +
                    ", instanceId=" + instanceId +
                    ", ballot=" + ballot +
                    '}';
        }
    }

    class PrepareResponse implements Event {
        private int sender;
        private long instanceId;
        private boolean success;
        private int maxBallot;
        private int acceptedBallot;
        private byte[] acceptedValue;

        public PrepareResponse(int sender, long instanceId, boolean success, int maxBallot, int acceptedBallot, byte[] acceptedValue) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.success = success;
            this.maxBallot = maxBallot;
            this.acceptedBallot = acceptedBallot;
            this.acceptedValue = acceptedValue;
        }

        @Override
        public Code code() {
            return Code.PREPARE_RESPONSE;
        }

        @Override
        public int senderId() {
            return this.sender;
        }

        @Override
        public long instanceId() {
            return this.instanceId;
        }

        public boolean success(){
            return this.success;
        }

        public int maxBallot() {
            return this.maxBallot;
        }

        public int acceptedBallot() {
            return this.acceptedBallot;
        }

        public byte[] acceptedValue() {
            return this.acceptedValue;
        }

        @Override
        public String toString() {
            return "PrepareResponse{" +
                    "sender=" + sender +
                    ", instanceId=" + instanceId +
                    ", success=" + success +
                    ", maxBallot=" + maxBallot +
                    ", acceptedBallot=" + acceptedBallot +
                    ", acceptedValue=" + Arrays.toString(acceptedValue) +
                    '}';
        }
    }

    class AcceptRequest implements Event {
        private int sender;
        private long instanceId;
        private int ballot;
        private byte[] value;

        public AcceptRequest(int sender, long instanceId, int ballot, byte[] value) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.ballot = ballot;
            this.value = value;
        }

        @Override
        public Code code() {
            return Code.ACCEPT;
        }

        @Override
        public int senderId() {
            return this.sender;
        }

        @Override
        public long instanceId() {
            return this.instanceId;
        }

        public int ballot() {
            return this.ballot;
        }

        public byte[] value() {
            return this.value;
        }
    }

    class AcceptResponse implements Event {
        private int sender;
        private long instanceId;
        private int maxBallot;
        private boolean accepted;

        public AcceptResponse(int sender, long instanceId, int maxBallot, boolean accepted) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.maxBallot = maxBallot;
            this.accepted = accepted;
        }

        @Override
        public Code code() {
            return Code.ACCEPT;
        }

        @Override
        public int senderId() {
            return this.sender;
        }

        @Override
        public long instanceId() {
            return this.instanceId;
        }

        public int maxBallot() {
            return this.maxBallot;
        }

        public boolean accepted(){
            return this.accepted;
        }
    }
}
