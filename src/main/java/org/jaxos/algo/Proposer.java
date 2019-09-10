package org.jaxos.algo;

import com.google.protobuf.ByteString;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.jaxos.JaxosConfig;
import org.jaxos.base.EntryOnce;
import org.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class Proposer {
    public static final int NONE = 0;
    public static final int PREPARING = 1;
    public static final int ACCEPTING = 2;
    public static final int CHOSEN = 3;
    private static Logger logger = LoggerFactory.getLogger(Proposer.class);

    private JaxosConfig config;
    private Supplier<Communicator> communicator;

    private InstanceContext instanceContext;
    private volatile long proposeInstanceId;
    private AtomicInteger state;
    private volatile int ballot;
    private volatile ByteString proposeValue;
    private AtomicInteger totalMaxBallot = new AtomicInteger(0);
    private AtomicReference<AcceptedValue> maxAcceptedValue = new AtomicReference<>(AcceptedValue.NONE);
    private IntBitSet repliedNodes = new IntBitSet();
    private volatile boolean allAccept;
    private volatile boolean switching;

    private volatile boolean historical;
    private volatile ByteString chosenValue = ByteString.EMPTY;

    private EntryOnce preparedEntry = new EntryOnce();
    private EntryOnce acceptedEntry = new EntryOnce();

    private HashedWheelTimer timer = new HashedWheelTimer(3, TimeUnit.SECONDS);
    private Timeout timeout;

    private Object executingSignal = new Object();
    private volatile ProposeResult result = null;

    private volatile long times0 = 0;
    private volatile long nanos0 = 0;

    public Proposer(JaxosConfig config, InstanceContext instanceContext, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.state = new AtomicInteger(NONE);
        this.instanceContext = instanceContext;
    }

    public synchronized ProposeResult propose(ByteString value) throws InterruptedException {
        if (!communicator.get().available()) {
            return ProposeResult.NO_QUORUM;
        }

        final long startNano = System.nanoTime();
        this.proposeValue = value;
        this.result = null;
        this.state.set(NONE);
        if (instanceContext.getLastRequestRecord().serverId() == config.serverId()) {
            logger.debug("I am leader, do accept directly");
            askAccept(this.config.serverId(), value, NONE);
        }
        else {
            askPrepare(this.config.serverId());
        }

        //process not end
        if (this.result == null) {
            synchronized (executingSignal) {
                executingSignal.wait();
            }
        }

        JaxosMetrics m = this.instanceContext.jaxosMetrics();
        m.recordPropose(System.nanoTime() - startNano);

        if (this.instanceContext.lastInstanceId() % 1000 == 0) {
            long timesDelta = m.proposeTimes() - this.times0;
            double avgNanos = (m.proposeTotalNanos() - this.nanos0)/(double)timesDelta;

            logger.info("total propose times is {}, average elapsed {} ms in recent {} times", m.proposeTimes(), avgNanos / 1e+6, timesDelta);

            this.times0 = m.proposeTimes();
            this.nanos0 = m.proposeTotalNanos();
        }
        return this.result;
    }

    private void endWith(ProposeResult result) {
        synchronized (this.executingSignal) {
            this.result = result;
            this.executingSignal.notifyAll();
        }
    }

    private void askPrepare(int ballot0) {
        resetState(NONE, PREPARING, ballot0);
        this.proposeInstanceId = instanceContext.lastInstanceId() + 1;
        Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), this.proposeInstanceId, this.ballot);

        timeout = timer.newTimeout(t -> {
            logger.info("prepare timeout");

        }, 3, TimeUnit.SECONDS);
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
                this.repliedNodes.clear();
                this.allAccept = true;
                this.historical = false;
                this.chosenValue = ByteString.EMPTY;
                this.result = null;
                return true;
            }
            return false;
        }
        finally {
            this.switching = false;
        }
    }

    public void onPrepareReply(Event.PrepareResponse response) {
        logger.debug("got PREPARE reply {}", response);

        if (this.switching) {
            logger.debug("Abandon response at switching");
            return;
        }

        if (state.get() != PREPARING) {
            logger.warn("receive PREPARE reply not preparing {}", response);
            return;
        }

        if (repliedNodes.get(response.senderId())) {
            logger.warn("Duplicated PREPARE response {}", response);
            return;
        }
        repliedNodes.add(response.senderId());

        if (response.acceptedBallot() == Integer.MAX_VALUE) { //Instance has been chosen
            this.historical = true;
            this.chosenValue = response.acceptedValue();
        }
        else {
            accumulateTotalMaxBallot(response.maxBallot());
            if (response.acceptedBallot() > this.maxAcceptedValue.get().ballot) {
                accumulateAcceptedValue(response.acceptedBallot(), response.acceptedValue());
            }
        }

        int n = repliedNodes.count();
        if (n == config.peerCount()) {
            logger.debug("End prepare after receiving {} response ", n);
            timeout.cancel();
            preparedEntry.exec(this::onPrepareEnd);
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
            logger.info("There are another accepted value \"{}\"", value.toStringUtf8()); //FIXME not string in future
        } while (!this.maxAcceptedValue.compareAndSet(v0, v1));
    }

    private boolean checkMajority() {
        int n = repliedNodes.count();
        if (n <= this.config.peerCount() / 2) {
            endWith(ProposeResult.NO_QUORUM);
            return false;
        }
        return true;
    }

    private void onPrepareEnd() {
        if (!checkMajority()) {
            return;
        }

        if (historical) {
            logger.debug("on all prepare, forward instance id to {}", this.proposeInstanceId);
            instanceContext.learnValue(this.proposeInstanceId, this.chosenValue);
            this.state.set(NONE);
            askPrepare(config.serverId());
            return;
        }
        AcceptedValue mv = this.maxAcceptedValue.get();

        logger.debug("on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                this.maxAcceptedValue.get().ballot, mv.ballot, this.ballot);

        int maxiBallot = this.totalMaxBallot.get();
        int b0 = this.ballot;
        int newBallot = maxiBallot > b0 ? nextBallot(maxiBallot) : b0;


        ByteString value;
        if (mv.ballot == 0) {
            logger.debug("No other chose value, use mine");
            value = this.proposeValue;
        }
        else {
            logger.debug("use another allAccept value");
            value = mv.content;
        }

        askAccept(newBallot, value, PREPARING);
    }

    private void askAccept(int newBallot, ByteString value, int s0) {
        if (!resetState(s0, ACCEPTING, newBallot)) {
            logger.debug("Another thread has handle the state change");
            return;
        }
        //prepare phase ignored
        if (s0 == NONE) {
            this.proposeInstanceId = instanceContext.lastInstanceId() + 1;
        }
        logger.debug("new ballot is {} and my ballot changed to {}", newBallot, this.ballot);
        Event.AcceptRequest request = new Event.AcceptRequest(this.config.serverId(), this.proposeInstanceId, this.ballot, value);
        this.timeout = timer.newTimeout(t -> onAcceptEnd(), 3, TimeUnit.SECONDS);
        this.communicator.get().broadcast(request);
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        logger.debug("got ACCEPT reply {}", response);
        if (switching) {
            logger.info("Abandon response when switching");
            return;
        }
        if (state.get() != ACCEPTING) {
            logger.warn("Not accepting state, abandon the response");
            return;
        }
        if (this.repliedNodes.get(response.senderId())) {
            logger.warn("Duplicated ACCEPT response {}", response);
            return;
        }

        repliedNodes.add(response.senderId());

        if (!response.accepted()) {
            this.allAccept = false;
        }

        if (repliedNodes.count() == config.peerCount()) {
            this.timeout.cancel();
            acceptedEntry.exec(this::onAcceptEnd);
        }
    }

    private void onAcceptEnd() {
        if (!this.checkMajority()) {
            return;
        }

        logger.debug("Received all accept response");
        if (this.allAccept) {
            Event notify = new Event.ChosenNotify(this.config.serverId(), this.proposeInstanceId, this.ballot);
            this.communicator.get().broadcast(notify);
            state.set(CHOSEN);
            endWith(ProposeResult.SUCCESS);
        }
        else {
            state.set(NONE);
            askPrepare(nextBallot(this.totalMaxBallot.get()));
        }
    }

    private int nextBallot(int b) {
        return nextBallot(b, 10, this.config.serverId());
    }

    private int nextBallot(int b, int m, int id) {
        return ((b / m) + 1) * m + id;
    }
}
