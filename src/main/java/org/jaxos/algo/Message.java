package org.jaxos.algo;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Message {
    enum Code {
        PREPARE, PREPARE_RESPONSE, ACCEPT, ACCEPT_RESPONSE
    }

    public interface PaxosMessage {
        Code code();
        String sender();
        long instanceId();
    }

    public static class Prepare implements PaxosMessage {
        private String sender;
        private long instanceId;
        private int ballot;

        public Prepare(String sender, long instanceId, int ballot) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.ballot = ballot;
        }

        @Override
        public Code code() {
            return Code.PREPARE;
        }

        @Override
        public String sender() {
            return sender;
        }

        @Override
        public long instanceId() {
            return instanceId;
        }

        public int ballot() {
            return ballot;
        }
    }

    public static class PrepareResponse implements PaxosMessage {
        private String sender;
        private long instanceId;
        private int maxBallot;
        private byte[] acceptedValue;

        public PrepareResponse(String sender, long instanceId, int maxBallot, byte[] acceptedValue) {
            this.sender = sender;
            this.instanceId = instanceId;
            this.maxBallot = maxBallot;
            this.acceptedValue = acceptedValue;
        }

        @Override
        public Code code() {
            return Code.PREPARE_RESPONSE;
        }

        @Override
        public String sender() {
            return this.sender;
        }

        @Override
        public long instanceId() {
            return this.instanceId;
        }

        public int maxBallot() {
            return this.maxBallot;
        }

        public byte[] acceptedValue() {
            return this.acceptedValue;
        }
    }

    public static class Accept implements PaxosMessage {
        private String sender;
        private long instanceId;
        private int ballot;
        private byte[] value;

        public Accept(String sender, long instanceId, int ballot, byte[] value) {
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
        public String sender() {
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

    public static class AcceptResponse implements PaxosMessage {
        private String sender;
        private long instanceId;
        private int maxBallot;
        private boolean accepted;

        public AcceptResponse(String sender, long instanceId, int maxBallot, boolean accepted) {
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
        public String sender() {
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
