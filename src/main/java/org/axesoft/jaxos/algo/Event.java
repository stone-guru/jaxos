package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public abstract class Event {
    public enum Code {
        NOOP, HEART_BEAT, HEART_BEAT_RESPONSE, PREPARE, PREPARE_RESPONSE, ACCEPT, ACCEPT_RESPONSE,
        ACCEPTED_NOTIFY, ACCEPTED_NOTIFY_RESPONSE, PREPARE_TIMEOUT, ACCEPT_TIMEOUT
    }

    //FIXME refact to enum
    public static final int RESULT_REJECT = 0;
    public static final int RESULT_SUCCESS = 1;
    public static final int RESULT_STANDBY = 3;

    private int senderId;
    private int squadId;
    private long instanceId;
    private int round;

    public Event(int senderId, int squadId, long instanceId, int round) {
        this.senderId = senderId;
        this.squadId = squadId;
        this.instanceId = instanceId;
        this.round = round;
    }

    abstract public Code code();

    public int senderId() {
        return this.senderId;
    }

    public int squadId() {
        return this.squadId;
    }

    public long instanceId() {
        return this.instanceId;
    }

    public int round() {
        return this.round;
    }


    @Override
    public String toString() {
        return "senderId=" + senderId +
                ", squadId=" + squadId +
                ", instanceId=" + instanceId +
                ", round=" + round;
    }

    public static class HeartBeatRequest extends Event {
        public HeartBeatRequest(int senderId) {
            super(senderId, 0, 0, 0);
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT;
        }
    }

    public static class HeartBeatResponse extends Event {
        public HeartBeatResponse(int senderId) {
            super(senderId, 0, 0, 0);
            super.senderId = senderId;
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT_RESPONSE;
        }
    }

    public static class PrepareRequest extends Event {
        private int ballot;
        private int lastChosenBallot;

        public PrepareRequest(int sender, int squadId, long instanceId, int round, int ballot, int lastChosenBallot) {
            super(sender, squadId, instanceId, round);
            this.ballot = ballot;
            this.lastChosenBallot = lastChosenBallot;
        }

        @Override
        public Code code() {
            return Code.PREPARE;
        }


        public int ballot() {
            return ballot;
        }

        public int lastChosenBallot(){
            return this.lastChosenBallot;
        }

        @Override
        public String toString() {
            return "PrepareRequest{" + super.toString() +
                    ", ballot=" + this.ballot +
                    ", lastChosenBallot=" + this.lastChosenBallot +
                    '}';
        }
    }


    public static class PrepareResponse extends Event {
        private int result;
        private int maxBallot;
        private int acceptedBallot;
        private ByteString acceptedValue;
        private long chosenInstanceId;

        public static class Builder {
            private PrepareResponse resp;

            public Builder(int sender, int squadId, long instanceId, int round) {
                resp = new PrepareResponse(sender, squadId, instanceId, round);
            }

            public Builder setResult(int result) {
                resp.result = result;
                return this;
            }

            public Builder setMaxProposal(int proposal) {
                resp.maxBallot = proposal;
                return this;
            }

            public Builder setAccepted(int proposal, ByteString value) {
                resp.acceptedBallot = proposal;
                resp.acceptedValue = checkNotNull(value);
                return this;
            }

            public Builder setChosenInstanceId(long i) {
                resp.chosenInstanceId = i;
                return this;
            }

            public PrepareResponse build() {
                return resp;
            }
        }

        private PrepareResponse(int sender, int squadId, long instanceId, int round) {
            super(sender, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.PREPARE_RESPONSE;
        }

        public int result() {
            return this.result;
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

        public long chosenInstanceId() {
            return this.chosenInstanceId;
        }

        @Override
        public String toString() {
            return "PrepareResponse{" + super.toString() +
                    ", result =" + result +
                    ", maxBallot=" + maxBallot +
                    ", acceptedBallot=" + acceptedBallot +
                    ", acceptedValue=B[" + acceptedValue.size() + "]" +
                    ", chosenInstanceId=" + chosenInstanceId +
                    '}';
        }
    }

    public static class AcceptRequest extends Event {
        private int ballot;
        private int lastChosenBallot;
        private ByteString value;

        public AcceptRequest(int sender, int squadId, long instanceId, int round, int ballot, ByteString value, int lastChosenBallot) {
            super(sender, squadId, instanceId, round);
            this.ballot = ballot;
            this.value = checkNotNull(value);
            this.lastChosenBallot = lastChosenBallot;
        }

        @Override
        public Code code() {
            return Code.ACCEPT;
        }

        public int ballot() {
            return this.ballot;
        }

        public ByteString value() {
            return this.value;
        }

        public int lastChosenBallot() {
            return lastChosenBallot;
        }

        @Override
        public String toString() {
            return "AcceptRequest{" + super.toString() +
                    ", ballot=" + ballot +
                    ", value=B[" + value.size() + "]" +
                    ", lastChosenBallot=" + lastChosenBallot +
                    '}';
        }

    }

    public static class AcceptResponse extends Event {
        private int maxBallot;
        private int result;
        private long chosenInstanceId;

        public AcceptResponse(int sender, int squadId, long instanceId, int round, int maxBallot, int result, long chosenInstanceId) {
            super(sender, squadId, instanceId, round);
            this.maxBallot = maxBallot;
            this.result = result;
            this.chosenInstanceId = chosenInstanceId;
        }

        @Override
        public Code code() {
            return Code.ACCEPT_RESPONSE;
        }

        public int maxBallot() {
            return this.maxBallot;
        }

        public int result() {
            return this.result;
        }

        public long chosenInstanceId() {
            return this.chosenInstanceId;
        }

        @Override
        public String toString() {
            return "AcceptResponse{" + super.toString() +
                    ", maxBallot=" + maxBallot +
                    ", result=" + result +
                    ", chosenInstanceId=" + chosenInstanceId +
                    '}';
        }
    }

    public static class ChosenNotify extends Event {
        private int ballot;

        public ChosenNotify(int sender, int squadId, long instanceId, int ballot) {
            super(sender, squadId, instanceId, 0);
            this.ballot = ballot;
        }

        @Override
        public Code code() {
            return Code.ACCEPTED_NOTIFY;
        }

        public int ballot() {
            return this.ballot;
        }

        @Override
        public String toString() {
            return "AcceptedNotify{" + super.toString() +
                    ", ballot=" + ballot +
                    '}';
        }
    }

    public static class PrepareTimeout extends Event {
        public PrepareTimeout(int senderId, int squadId, long instanceId, int round) {
            super(senderId, squadId, instanceId, round);
        }
        @Override
        public Code code() {
            return Code.PREPARE_TIMEOUT;
        }


        @Override
        public String toString() {
            return "PrepareTimeout{" + super.toString() + "}";
        }
    }

    public static class AcceptTimeout extends Event {
        public AcceptTimeout(int senderId, int squadId, long instanceId, int round) {
            super(senderId, squadId, instanceId, round);
        }

        @Override
        public Code code() {
            return Code.ACCEPT_TIMEOUT;
        }

        @Override
        public String toString() {
            return "AcceptTimeout{" + super.toString() + "}";
        }
    }
}
