package org.jaxos.algo;

import org.apache.commons.lang3.time.StopWatch;
import org.jaxos.JaxosConfig;
import org.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class Proposal {
    public static final int NONE = 0;
    public static final int PREPARING = 1;
    public static final int ACCEPTING = 2;
    public static final int ACCEPTED = 3;


    public static class AcceptedValue {
        public final int ballot;
        public final byte[] content;

        public AcceptedValue(int ballot, byte[] content) {
            this.ballot = ballot;
            this.content = content;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(Proposal.class);

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final AcceptedValue NONE_VALUE = new AcceptedValue(0, EMPTY_BYTES);

    private JaxosConfig config;
    private Supplier<Communicator> communicator;

    private AtomicInteger state;
    private volatile int ballot;
    private byte[] proposeValue;
    private AtomicInteger totalMaxBallot = new AtomicInteger(0);
    private AtomicReference<AcceptedValue> maxAcceptedValue = new AtomicReference<>(NONE_VALUE);
    private AtomicInteger repliedCount = new AtomicInteger(0);
    private IntBitSet replied = new IntBitSet();
    private volatile boolean allAccept;
    private volatile boolean switching;

    private StopWatch watch;
    private AtomicLong taskElapsed = new AtomicLong(0);

    public Proposal(JaxosConfig config, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.state = new AtomicInteger(NONE);
    }

    public void propose(byte[] value) {
        this.proposeValue = value;
        this.watch = StopWatch.createStarted();
        this.state.set(NONE);
        askPrepare(this.config.serverId());
    }

    private void askPrepare(int ballot0) {
        resetState(NONE, PREPARING, ballot0);
        Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), 1000, this.ballot);
        communicator.get().broadcast(req);
    }

    private boolean resetState(int s0, int s1, int ballot0) {
        if (this.switching) {
            return false;
        }
        this.switching = true;
        try {
            if (this.state.compareAndSet(s0, s1)) {
                this.totalMaxBallot.set(0);
                this.maxAcceptedValue.set(NONE_VALUE);
                this.ballot = ballot0;
                this.replied.clear();
                this.repliedCount.set(0);
                this.allAccept = true;
                return true;
            }
            return false;
        }
        finally {
            this.switching = false;
        }
    }

    public void onPrepareReply(Event.PrepareResponse response) {
        logger.info("got PREPARE reply {}", response);

        if (this.switching) {
            logger.info("Abandon response at switching");
            return;
        }

        if (state.get() != PREPARING) {
            logger.warn("receive PREPARE reply not preparing {}", response);
            return;
        }

        if (replied.get(response.senderId())) {
            logger.warn("Duplicated PREPARE response {}", response);
            return;
        }
        replied.set(response.senderId());
        repliedCount.incrementAndGet();

        accumulateTotalMaxBallot(response.maxBallot());
        if(response.acceptedBallot() > this.maxAcceptedValue.get().ballot){
            accumulateAcceptedValue(response.acceptedBallot(), response.acceptedValue());
        }

        if (repliedCount.get() == config.peerCount()) {
            logger.info("End prepare after receiving {} response ", repliedCount.get());
            askAccept();
        }
    }

    private void accumulateTotalMaxBallot(int ballot) {
        int b0;
        do{
            b0 = this.totalMaxBallot.get();
            if(b0 > ballot){
                return;
            }
        }while(!this.totalMaxBallot.compareAndSet(b0, ballot));
    }

    private void accumulateAcceptedValue(int ballot, byte[] value){
        AcceptedValue v0, v1;
        do{
            v0 = this.maxAcceptedValue.get();
            if(v0.ballot >= ballot){
                return;
            }
            v1 = new AcceptedValue(ballot, value);
            logger.info("There are another allAccept value \"{}\"", new String(value));
        }while(!this.maxAcceptedValue.compareAndSet(v0, v1));
    }

    private void askAccept() {
        logger.info("askAccept max allAccept ballot = {}", this.maxAcceptedValue.get().ballot);
        AcceptedValue mv = this.maxAcceptedValue.get();
        int newBallot = this.totalMaxBallot.get() >= this.ballot ? nextBalot(this.totalMaxBallot.get()) : this.ballot;

        if (!resetState(PREPARING, ACCEPTING, newBallot)){
            logger.info("Another thread has handle the state change");
            return;
        }

        logger.warn("Prepare end in {} ms", watch.getTime(TimeUnit.MILLISECONDS));

        byte[] value;
        if (mv.ballot == 0) {
            logger.info("No allAccept value, use mine");
            value = this.proposeValue;
        }
        else {
            logger.info("use another allAccept value");
            value = mv.content;
        }

        Event.AcceptRequest request = new Event.AcceptRequest(this.config.serverId(), 1000, this.ballot, value);
        this.communicator.get().broadcast(request);
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        logger.info("got ACCEPT reply {}", response);
        if (switching) {
            logger.info("Abandon response when switching");
            return;
        }

        if (state.get() != ACCEPTING) {
            logger.warn("Not accepting state, abandom the response");
            return;
        }

        if (this.replied.get(response.senderId())) {
            logger.warn("Duplicated ACCEPT response {}", response);
            return;
        }
        replied.set(response.senderId());
        repliedCount.incrementAndGet();

        if(!response.accepted()){
            this.allAccept = false;
        }

        if (repliedCount.get() < config.peerCount()) {
            return;
        }

        logger.info("Received all accept resonses");
        if (this.allAccept) {
            state.set(ACCEPTED);
            long t = watch.getTime(TimeUnit.MILLISECONDS);
            logger.warn("value accepted in  {} ms", t);
            this.taskElapsed.addAndGet(t);
        }
        else {
            state.set(NONE);
            askPrepare(nextBalot(this.totalMaxBallot.get()));
        }
    }

    public boolean accepted(){
        return state.get() == ACCEPTED;
    }

    public long taskElpasedMillis(){
        return taskElapsed.get();
    }

    private int nextBalot(int b) {
        return nextBalot(b, 10, this.config.serverId());
    }

    private int nextBalot(int b, int m, int id) {
        return ((b / m) + 1) * m + id;
    }
}
