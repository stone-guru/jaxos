package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosConfig;
import org.axesoft.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private volatile ByteString proposeValue;

    private HashedWheelTimer timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);
    private Timeout timeout;


    private volatile PrepareActor prepareActor;
    private volatile Timeout prepareTimeout;
    private volatile AcceptActor acceptActor;
    private volatile Timeout acceptTimeout;

    private Object executingSignal = new Object();
    private volatile ProposeResult result = null;

    private volatile long times0 = 0;
    private volatile long nanos0 = 0;

    public Proposer(JaxosConfig config, InstanceContext instanceContext, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.instanceContext = instanceContext;
    }

    public synchronized ProposeResult propose(ByteString value) throws InterruptedException {
        if (!communicator.get().available()) {
            return ProposeResult.NO_QUORUM;
        }

        final long startNano = System.nanoTime();
        this.proposeValue = value;
        this.result = null;

        this.timeout = timer.newTimeout(this::executingTimeout, 3, TimeUnit.SECONDS);

        if (this.instanceContext.isLeader()) {
            startAccept(this.instanceContext.lastInstanceId() + 1, this.proposeValue, this.config.serverId());
        }
        else {
            startPrepare(this.config.serverId());
        }

        //process not end
        while (this.result == null) {
            synchronized (executingSignal) {
                executingSignal.wait();
            }
        }
        recordMetrics(startNano);

        return this.result;
    }

    private void endWith(ProposeResult r) {
        synchronized (this.executingSignal) {
            this.timeout.cancel();
            this.result = r;
            this.prepareActor = null;
            this.acceptActor = null;
            this.executingSignal.notifyAll();
        }
    }

    private void executingTimeout(Timeout t) {
        logger.error("executing not finished in time");
        endWith(ProposeResult.TIME_OUT);
    }

    private boolean checkMajority(int n) {
        if (n <= this.config.peerCount() / 2) {
            endWith(ProposeResult.NO_QUORUM);
            return false;
        }
        return true;
    }

    private void recordMetrics(long startNano) {
        JaxosMetrics m = this.instanceContext.jaxosMetrics();
        m.recordPropose(System.nanoTime() - startNano);

        if (this.instanceContext.lastInstanceId() % 1000 == 0) {
            long timesDelta = m.proposeTimes() - this.times0;
            double avgNanos = (m.proposeTotalNanos() - this.nanos0) / (double) timesDelta;

            logger.info("{} total propose times is {}, average elapsed {} ms in recent {} times",
                    new Date(), m.proposeTimes(), avgNanos / 1e+6, timesDelta);

            this.times0 = m.proposeTimes();
            this.nanos0 = m.proposeTotalNanos();
        }
    }

    private void startPrepare(int proposal0) {
        this.prepareActor = new PrepareActor(this.instanceContext.lastInstanceId() + 1, this.proposeValue, proposal0);

        this.prepareTimeout = this.timer.newTimeout(t -> this.endPrepare(), 1, TimeUnit.SECONDS);
        this.prepareActor.begin();
    }


    public void onPrepareReply(Event.PrepareResponse response) {
        logger.debug("RECEIVED {}", response);

        PrepareActor actor = this.prepareActor;
        if (actor == null) {
            logger.warn("Not at state of preparing for {}", response);
            return;
        }

        actor.onReply(response);
        if (actor.isAllReplied()) {
            this.prepareTimeout.cancel();
            endPrepare();
        }
    }

    private void endPrepare() {
        PrepareActor actor = this.prepareActor;
        actor.once(() -> {
            if (!checkMajority(actor.repliedNodes.count())) {
                return;
            }

            if (actor.isAllAccepted()) {
                ValueWithProposal v = actor.getResult();
                startAccept(actor.instanceId, v.content, v.ballot);
            }
            else {
                int p = nextProposal(actor.totalMaxProposal());
                startPrepare(p);
            }
        });
    }

    private void startAccept(long instanceId, ByteString value, int proposal) {
        this.acceptActor = new AcceptActor(instanceId, value, proposal);
        this.acceptTimeout = this.timer.newTimeout(t -> endAccept(), 1, TimeUnit.SECONDS);
        this.acceptActor.begin();
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        logger.debug("RECEIVED {}", response);

        AcceptActor actor = this.acceptActor;
        if (actor == null) {
            logger.warn("Not at state of accepting for {}", response);
            return;
        }

        actor.onReply(response);
        if (actor.isAllReplied()) {
            endAccept();
        }
    }

    private void endAccept() {
        logger.debug("End Accept");
        this.acceptTimeout.cancel();
        AcceptActor actor = this.acceptActor;
        if(actor == null){
            return; //already executed
        }
        actor.once(() -> {
            if (!checkMajority(actor.repliedNodes.count())) {
                return;
            }

            if (actor.isAllAccepted()) {
                actor.notifyChosen();
                endWith(ProposeResult.success(actor.instanceId));
            }
            else {
                int newProposal = nextProposal(actor.maxProposal());
                startPrepare(newProposal);
            }
        });
    }

    private int nextProposal(int b) {
        return nextProposal(b, 10, this.config.serverId());
    }

    private static int nextProposal(int b, int m, int id) {
        return ((b / m) + 1) * m + id;
    }


    private static void accumulateMax(AtomicInteger a, int v) {
        int v0;
        do {
            v0 = a.get();
            if (v >= v0) {
                return;
            }
        } while (!a.compareAndSet(v0, v));
    }


    private class ActorBase {
        private final AtomicBoolean done = new AtomicBoolean(false);

        public void once(Runnable r) {
            if (!done.get()) {
                if (done.compareAndSet(false, true)) {
                    r.run();
                }
            }
        }
    }

    private class PrepareActor extends ActorBase {
        private long instanceId;
        private int proposal;
        private ByteString value;
        private AtomicInteger totalMaxProposal = new AtomicInteger(0);
        private AtomicReference<ValueWithProposal> maxAcceptedValue = new AtomicReference<>(ValueWithProposal.NONE);
        private IntBitSet repliedNodes = new IntBitSet();
        private volatile boolean allAccepted = true;
        private boolean historical = true;
        private ByteString chosenValue = ByteString.EMPTY;


        public PrepareActor(long instanceId, ByteString value, int proposal) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
        }

        public void begin() {
            Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), this.instanceId, this.proposal);
            communicator.get().broadcast(req);
        }

        public void onReply(Event.PrepareResponse response) {
            logger.debug("got PREPARE reply {}", response);

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
                accumulateMax(this.totalMaxProposal, response.maxBallot());
                if (response.acceptedBallot() > this.maxAcceptedValue.get().ballot) {
                    accumulateAcceptedValue(response.acceptedBallot(), response.acceptedValue());
                }
            }

            if (!response.success()) {
                allAccepted = false;
            }
        }

        public boolean isAllReplied() {
            return repliedNodes.count() == config.peerCount();
        }

        public boolean isAllAccepted() {
            return this.allAccepted;
        }

        public int totalMaxProposal() {
            return this.totalMaxProposal.get();
        }

        private ValueWithProposal getResult() {
            ValueWithProposal mv = this.maxAcceptedValue.get();

            logger.debug("on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                    this.maxAcceptedValue.get().ballot, mv.ballot, this.proposal);

            int maxiBallot = this.totalMaxProposal.get();
            int b0 = this.proposal;
            int newBallot = maxiBallot > b0 ? nextProposal(maxiBallot) : b0;

            ByteString value;
            if (mv.ballot == 0) {
                logger.debug("No other chose value, use mine");
                value = this.value;
            }
            else {
                logger.debug("use another allAccept value");
                value = mv.content;
            }

            return new ValueWithProposal(newBallot, value);
        }


        private void accumulateAcceptedValue(int proposal, ByteString value) {
            ValueWithProposal v0, v1;
            do {
                v0 = this.maxAcceptedValue.get();
                if (v0.ballot >= proposal) {
                    return;
                }
                v1 = new ValueWithProposal(proposal, value);
                logger.info("There are another accepted value \"{}\"", value.toStringUtf8()); //FIXME not string in future
            } while (!this.maxAcceptedValue.compareAndSet(v0, v1));
        }
    }


    private class AcceptActor extends ActorBase {
        private final long instanceId;
        private final int proposal;
        private final ByteString value;
        private final AtomicInteger maxProposal = new AtomicInteger(0);
        private volatile boolean allAccepted = true;
        private IntBitSet repliedNodes = new IntBitSet();

        public AcceptActor(long instanceId, ByteString value, int proposal) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
        }

        public void begin() {
            logger.debug("start accept with proposal  {} ", this.proposal);
            Event.AcceptRequest request = new Event.AcceptRequest(config.serverId(), this.instanceId, this.proposal, this.value);
            Proposer.this.communicator.get().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response) {
            if (this.repliedNodes.get(response.senderId())) {
                logger.warn("Duplicated ACCEPT response {}", response);
                return;
            }

            this.repliedNodes.add(response.senderId());
            if (!response.accepted()) {
                this.allAccepted = false;
            }
            accumulateMax(this.maxProposal, response.maxBallot());
        }

        public boolean isAllAccepted() {
            return this.allAccepted;
        }

        public boolean isAllReplied() {
            return this.repliedNodes.count() == config.peerCount();
        }

        public int maxProposal() {
            return maxProposal.get();
        }

        public void notifyChosen() {
            logger.debug("Notify instance {} chosen", this.instanceId);
            Event notify = new Event.ChosenNotify(config.serverId(), this.instanceId, this.proposal);
            communicator.get().broadcast(notify);
        }
    }

}
