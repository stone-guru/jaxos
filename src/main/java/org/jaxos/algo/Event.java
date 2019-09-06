package org.jaxos.algo;

import com.google.protobuf.ByteString;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public interface Event {
    enum Code {
        NOOP, HEART_BEAT, HEART_BEAT_RESPONSE, PREPARE, PREPARE_RESPONSE, ACCEPT, ACCEPT_RESPONSE,
        ACCEPTED_NOTIFY
    }

    Code code();
    int senderId();
    long instanceId();

    class HeartBeatRequest implements Event {
        private final int senderId;

        public HeartBeatRequest(int senderId) {
            this.senderId = senderId;
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT;
        }

        @Override
        public int senderId() {
            return this.senderId;
        }

        @Override
        public long instanceId() {
            return 0;
        }
    }

    class HeartBeatResponse implements Event {
        private final int senderId;

        public HeartBeatResponse(int senderId) {
            this.senderId = senderId;
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT_RESPONSE;
        }

        @Override
        public int senderId() {
            return this.senderId;
        }

        @Override
        public long instanceId() {
            return 0;
        }
    }

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
        private ByteString acceptedValue;

        public PrepareResponse(int sender, long instanceId, boolean success, int maxBallot, int acceptedBallot, ByteString acceptedValue) {
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

        public ByteString acceptedValue() {
            return this.acceptedValue;
        }

        @Override
        public String toString() {
            return "PrepareResponse{" +
                    "sender=" + sender +
                    ", instanceId=" + instanceId +
                    ", success=" + success +
                    ", ballot=" + maxBallot +
                    ", acceptedBallot=" + acceptedBallot +
                    ", acceptedValue=" + acceptedValue.toStringUtf8() + //FIXME not string in future
                    '}';
        }
    }

    class AcceptRequest implements Event {
        private int sender;
        private long instanceId;
        private int ballot;
        private ByteString value;

        public AcceptRequest(int sender, long instanceId, int ballot, ByteString value) {
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

        public ByteString value() {
            return this.value;
        }

        @Override
        public String toString() {
            return "AcceptRequest{" +
                    "sender=" + sender +
                    ", instanceId=" + instanceId +
                    ", ballot=" + ballot +
                    ", value=" + value.toStringUtf8() + //FIXME not string in future
                    '}';
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
            return Code.ACCEPT_RESPONSE;
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

        @Override
        public String toString() {
            return "AcceptResponse{" +
                    "sender=" + sender +
                    ", instanceId=" + instanceId +
                    ", ballot=" + maxBallot +
                    ", accepted=" + accepted +
                    '}';
        }
    }

    class ChosenNotify implements Event {
        private int sender;
        private long instanceId;
        private int ballot;

        public ChosenNotify(int sender, long instanceId, int ballot) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.ballot = ballot;
        }

        @Override
        public Code code() {
            return Code.ACCEPTED_NOTIFY;
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

        @Override
        public String toString() {
            return "AcceptedNotify{" +
                    "sender=" + sender +
                    ", instanceId=" + instanceId +
                    ", ballot=" + ballot +
                    '}';
        }
    }
}
