package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public abstract class Event {
    public enum Code {
        NOOP, HEART_BEAT, HEART_BEAT_RESPONSE, PREPARE, PREPARE_RESPONSE, ACCEPT, ACCEPT_RESPONSE,
        ACCEPTED_NOTIFY, ACCEPTED_NOTIFY_RESPONSE, PREPARE_TIMEOUT, ACCEPT_TIMEOUT,
        LEARN, LEARN_RESPONSE, CHOSEN_QUERY_CLICK, CHOSEN_QUERY, CHOSEN_QUERY_RESPONSE
    }

    //FIXME refact to enum
    public static final int RESULT_REJECT = 0;
    public static final int RESULT_SUCCESS = 1;
    public static final int RESULT_STANDBY = 3;

    private int senderId;
    private long timestamp;

    public Event(int sender){
        this(sender, System.currentTimeMillis());
    }

    public Event(int senderId, long timestamp) {
        this.senderId = senderId;
        this.timestamp = timestamp;
    }

    abstract public Code code();

    public int senderId() {
        return this.senderId;
    }

    public long timestamp(){
        return this.timestamp;
    }

    public int squadId(){
        return -1;
    }

    public static class HeartBeatRequest extends Event {
        public HeartBeatRequest(int senderId) {
            super(senderId);
        }

        public HeartBeatRequest(int senderId, long timestamp) {
            super(senderId, timestamp);
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT;
        }
    }

    public static class HeartBeatResponse extends Event {
        public HeartBeatResponse(int senderId, long timestamp) {
            super(senderId, timestamp);
        }

        public HeartBeatResponse(int senderId) {
            super(senderId);
        }

        @Override
        public Code code() {
            return Code.HEART_BEAT_RESPONSE;
        }
    }

    public static abstract class BallotEvent extends Event {
        private int squadId;
        private long instanceId;
        private int round;

        public BallotEvent(int senderId, int squadId, long instanceId, int round) {
            super(senderId);
            this.squadId = squadId;
            this.instanceId = instanceId;
            this.round = round;
        }

        @Override
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
            return "senderId=" + super.senderId +
                    ", squadId=" + squadId +
                    ", instanceId=" + instanceId +
                    ", round=" + round;
        }
    }

    public static class PrepareRequest extends BallotEvent {
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

        public int lastChosenBallot() {
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


    public static class PrepareResponse extends BallotEvent {
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

    public static class AcceptRequest extends BallotEvent {
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

    public static class AcceptResponse extends BallotEvent {
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

    public static class ChosenNotify extends BallotEvent {
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

    public static class PrepareTimeout extends BallotEvent {
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

    public static class AcceptTimeout extends BallotEvent {
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


    public static abstract class InstanceEvent extends Event {
        private int squadId;

        public InstanceEvent(int senderId, int squadId) {
            super(senderId);
            this.squadId = squadId;
        }

        public int squadId() {
            return this.squadId;
        }
    }

    public static class Learn extends InstanceEvent {
        private long highInstanceId;
        private long lowInstanceId;

        public Learn(int senderId, int squadId, long lowInstanceId, long highInstanceId) {
            super(senderId, squadId);
            this.highInstanceId = highInstanceId;
            this.lowInstanceId = lowInstanceId;
        }

        @Override
        public Code code() {
            return Code.LEARN;
        }

        public long lowInstanceId() {
            return this.lowInstanceId;
        }

        public long highInstanceId() {
            return this.highInstanceId;
        }

        @Override
        public String toString() {
            return "Learn{" +
                    "senderId=" + super.senderId() +
                    ", squadId=" + super.squadId() +
                    ", lowInstanceId=" + this.lowInstanceId +
                    ", highInstanceId=" + this.highInstanceId +
                    '}';
        }
    }

    public static class LearnResponse extends InstanceEvent {
        private List<Pair<Long, ByteString>> instances;

        public LearnResponse(int senderId, int squadId, List<Pair<Long, ByteString>> instances) {
            super(senderId, squadId);
            this.instances = checkNotNull(instances);
        }

        @Override
        public Code code() {
            return Code.LEARN_RESPONSE;
        }

        public List<Pair<Long, ByteString>> instances() {
            return this.instances;
        }

        @Override
        public String toString() {
            return "LearnResponse{" +
                    "senderId=" + super.senderId() +
                    ", squadId=" + super.squadId() +
                    ", instances=I[" + instances.size() + "]" +
                    '}';
        }

        public long lowInstanceId(){
            return instanceIdOf(0);
        }

        public long highInstanceId(){
            return instanceIdOf(instances.size() - 1);
        }

        private long instanceIdOf(int i){
            if(i >= 0 && i < instances.size()){
                return instances.get(i).getLeft();
            }
            return 0;
        }
    }
}
