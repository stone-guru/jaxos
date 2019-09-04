package org.jaxos.algo;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.time.StopWatch;
import org.jaxos.JaxosConfig;
import org.jaxos.base.EntryOnce;
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
    public static final int CHOSEN = 3;

    private static Logger logger = LoggerFactory.getLogger(Proposal.class);

    private JaxosConfig config;
    private Supplier<Communicator> communicator;

    private InstanceContext instanceContext;
    private volatile long proposeInstanceId;
    private AtomicInteger state;
    private volatile int ballot;
    private volatile ByteString proposeValue;
    private AtomicInteger totalMaxBallot = new AtomicInteger(0);
    private AtomicReference<AcceptedValue> maxAcceptedValue = new AtomicReference<>(AcceptedValue.NONE);
    private AtomicInteger repliedCount = new AtomicInteger(0);
    private IntBitSet replied = new IntBitSet();
    private volatile boolean allAccept;
    private volatile boolean switching;

    private volatile boolean historical;
    private volatile ByteString chosenValue = ByteString.EMPTY;

    private EntryOnce preparedEntry = new EntryOnce();
    private EntryOnce acceptedEntry = new EntryOnce();

    private StopWatch watch;
    private AtomicLong taskElapsed = new AtomicLong(0);

    public Proposal(JaxosConfig config, InstanceContext instanceContext, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.state = new AtomicInteger(NONE);
        this.instanceContext = instanceContext;
    }

    public void propose(ByteString value) {
        if(!communicator.get().available()){
            throw new CommunicatorException("Not enough nodes connected");
        }
        this.proposeValue = value;
        this.watch = StopWatch.createStarted();
        this.state.set(NONE);
        askPrepare(this.config.serverId());
    }

    private void askPrepare(int ballot0) {
        resetState(NONE, PREPARING, ballot0);
        this.proposeInstanceId = instanceContext.lastInstanceId() + 1;
        Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), this.proposeInstanceId, this.ballot);
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
                this.maxAcceptedValue.set(AcceptedValue.NONE);
                this.ballot = ballot0;
                this.replied.clear();
                this.repliedCount.set(0);
                this.allAccept = true;
                this.historical = false;
                this.chosenValue = ByteString.EMPTY;
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


        if (response.acceptedBallot() == Integer.MAX_VALUE) { //Instance has been chose
            this.historical = true;
            this.chosenValue = response.acceptedValue();
        }
        else {
            accumulateTotalMaxBallot(response.maxBallot());
            if (response.acceptedBallot() > this.maxAcceptedValue.get().ballot) {
                accumulateAcceptedValue(response.acceptedBallot(), response.acceptedValue());
            }
        }

        if (repliedCount.get() == config.peerCount()) {
            logger.info("End prepare after receiving {} response ", repliedCount.get());
            preparedEntry.exec(this::onAllPrepareReplied);
        }
    }

    private void accumulateTotalMaxBallot(int ballot) {
        int b0;
        do {
            b0 = this.totalMaxBallot.get();
            if (b0 > ballot) {
                return;
            }
        } while (!this.totalMaxBallot.compareAndSet(b0, ballot));
    }

    private void accumulateAcceptedValue(int ballot, ByteString value) {
        AcceptedValue v0, v1;
        do {
            v0 = this.maxAcceptedValue.get();
            if (v0.ballot >= ballot) {
                return;
            }
            v1 = new AcceptedValue(ballot, value);
            logger.info("There are another allAccept value \"{}\"", value.toStringUtf8()); //FIXME not string in future
        } while (!this.maxAcceptedValue.compareAndSet(v0, v1));
    }

    private void onAllPrepareReplied() {
        if (historical) {
            logger.debug("on all prepare, forward instance id to {}", this.proposeInstanceId);
            instanceContext.learnValue(this.proposeInstanceId, this.chosenValue);
            this.state.set(NONE);
            askPrepare(config.serverId());
            return;
        }

        logger.info("on all prepared max chose ballot = {}", this.maxAcceptedValue.get().ballot);
        AcceptedValue mv = this.maxAcceptedValue.get();
        int newBallot = this.totalMaxBallot.get() >= this.ballot ? nextBallot(this.totalMaxBallot.get()) : this.ballot;

        if (!resetState(PREPARING, ACCEPTING, newBallot)) {
            logger.info("Another thread has handle the state change");
            return;
        }

        logger.warn("Prepare end in {} ms", watch.getTime(TimeUnit.MILLISECONDS));

        ByteString value;
        if (mv.ballot == 0) {
            logger.info("No other chose value, use mine");
            value = this.proposeValue;
        }
        else {
            logger.info("use another allAccept value");
            value = mv.content;
        }

        Event.AcceptRequest request = new Event.AcceptRequest(this.config.serverId(), this.proposeInstanceId, this.ballot, value);
        this.communicator.get().broadcast(request);
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        logger.info("got ACCEPT reply {}", response);
        if (switching) {
            logger.info("Abandon response when switching");
            return;
        }
        if (state.get() != ACCEPTING) {
            logger.warn("Not accepting state, abandon the response");
            return;
        }
        if (this.replied.get(response.senderId())) {
            logger.warn("Duplicated ACCEPT response {}", response);
            return;
        }

        replied.set(response.senderId());
        repliedCount.incrementAndGet();

        if (!response.accepted()) {
            this.allAccept = false;
        }

        if (repliedCount.get() == config.peerCount()) {
            acceptedEntry.exec(this::afterAllAcceptReplied);
        }
    }

    private void afterAllAcceptReplied() {
        logger.info("Received all accept response");
        if (this.allAccept) {
            Event notify = new Event.ChosenNotify(this.config.serverId(), this.proposeInstanceId, this.ballot);
            this.communicator.get().broadcast(notify);
            long t = watch.getTime(TimeUnit.MILLISECONDS);
            logger.warn("value chose in  {} ms", t);
            state.set(CHOSEN);
            this.taskElapsed.addAndGet(t);
        }
        else {
            state.set(NONE);
            askPrepare(nextBallot(this.totalMaxBallot.get()));
        }
    }

    public boolean chosen() {
        return state.get() == CHOSEN;
    }

    public long taskElapsedMillis() {
        return taskElapsed.get();
    }

    private int nextBallot(int b) {
        return nextBallot(b, 10, this.config.serverId());
    }

    private int nextBallot(int b, int m, int id) {
        return ((b / m) + 1) * m + id;
    }
}
