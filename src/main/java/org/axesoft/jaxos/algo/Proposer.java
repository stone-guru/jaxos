package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosConfig;
import org.axesoft.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class Proposer {
    private static Logger logger = LoggerFactory.getLogger(Proposer.class);

    private JaxosConfig config;
    private Supplier<Communicator> communicator;
    private InstanceContext instanceContext;
    private HashedWheelTimer timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);

    private volatile long instanceId = 0;
    private volatile ByteString proposeValue;

    private volatile PrepareActor prepareActor;
    private volatile Timeout prepareTimeout;
    private volatile AcceptActor acceptActor;
    private volatile Timeout acceptTimeout;

    private CountDownLatch execEndLatch;
    private volatile ProposeResult result = null;

    private MetricsRecorder metricsRecorder;

    public Proposer(JaxosConfig config, InstanceContext instanceContext, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.instanceContext = instanceContext;
        this.metricsRecorder = new MetricsRecorder(instanceContext.jaxosMetrics());
    }

    public synchronized ProposeResult propose(ByteString value) throws InterruptedException {
        if (!communicator.get().available()) {
            return ProposeResult.NO_QUORUM;
        }

        final long startNano = System.nanoTime();
        this.proposeValue = value;

        this.result = null;
        this.execEndLatch = new CountDownLatch(1);

        if (this.instanceContext.isLeader() && (this == null)) {
            startAccept(this.instanceContext.lastInstanceId() + 1, this.proposeValue, this.config.serverId());
        }
        else {
            startPrepare(0, this.config.serverId(), 1);
        }

        this.execEndLatch.await(3, TimeUnit.SECONDS);

        if (this.result == null) {
            this.result = ProposeResult.TIME_OUT;
        }

        metricsRecorder.recordRoundExecMetrics(this.instanceId, startNano, this.result);

        return this.result;
    }

    private void endWith(ProposeResult r, String reason) {
        if (this.result != null) {
            return; //another thread has enter this
        }

        this.result = r;
        this.prepareActor = null;
        this.acceptActor = null;
        this.execEndLatch.countDown();

        logger.trace("propose round for {} end with {} by {}", this.instanceId, r.code(), reason);
    }

    private boolean checkMajority(int n, String step) {
        if (n <= this.config.peerCount() / 2) {
            endWith(ProposeResult.NO_QUORUM, step);
            return false;
        }
        return true;
    }

    private void startPrepare(long instanceId, int proposal0, int times) {
        long next = this.instanceContext.lastInstanceId() + 1;
        if (instanceId > 0 && instanceId != next) {
            endWith(ProposeResult.conflict(this.proposeValue), "when prepare " + next + " again");
            return;
        }

        this.prepareActor = new PrepareActor(next, this.proposeValue, proposal0, times);
        this.prepareTimeout = Proposer.this.timer.newTimeout(t -> this.endPrepare(this.prepareActor), 1, TimeUnit.SECONDS);
        this.prepareActor.begin();
    }


    public void processPrepareResponse(Event.PrepareResponse response) {
        logger.trace("RECEIVED {}", response);

        PrepareActor actor = this.prepareActor;
        if (actor == null) {
            logger.warn("Not at the state of preparing for {}", response);
            return;
        }

        actor.onReply(response);
        if (actor.isAllReplied()) {
            this.prepareTimeout.cancel();
            actor.once(() -> endPrepare(actor));
        }
    }

    private void endPrepare(PrepareActor actor) {
        //this.prepareActor = null;

        if (!checkMajority(actor.repliedNodes.count(), "PREPARE")) {
            return;
        }

        if (actor.isAllAccepted()) {
            ValueWithProposal v = actor.getResult();
            startAccept(actor.instanceId, v.content, actor.proposal);
        }
        else {
            ValueWithProposal v = actor.getResult();
            if (v.ballot == Integer.MAX_VALUE) {
                endWith(ProposeResult.conflict(this.proposeValue), "CONFLICT other value chosen");
                return;
            }
            if (actor.times >= 2) {
                sleepRandom("PREPARE");
            }
            startPrepare(actor.instanceId, nextProposal(actor.totalMaxProposal), actor.times + 1);
        }
    }

    private void startAccept(long instanceId, ByteString value, int proposal) {
        this.acceptActor = new AcceptActor(instanceId, value, proposal);
        this.acceptTimeout = this.timer.newTimeout(t -> endAccept(), 1, TimeUnit.SECONDS);
        this.acceptActor.begin();
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        logger.trace("RECEIVED {}", response);

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
        logger.trace("End Accept");
        this.acceptTimeout.cancel();
        AcceptActor actor = this.acceptActor;
        if (actor == null) {
            return; //already executed
        }
        actor.once(() -> {
            if (!checkMajority(actor.repliedNodes.count(), "ACCEPT")) {
                return;
            }

            if (actor.isAllAccepted()) {
                actor.notifyChosen();
                endWith(ProposeResult.success(actor.instanceId), "OK");
            }
            else if (actor.isChosenByOther()) {
                endWith(ProposeResult.conflict(this.proposeValue), "Chosen by other at accept");
            }
            else if (this.prepareActor.times >= 2) {
                endWith(ProposeResult.conflict(this.proposeValue), "REJECT at accept");
            }
            else {
                //uer another prepare phase to detect, whether this value chosen
                startPrepare(this.prepareActor.instanceId, this.config.serverId(), this.prepareActor.times + 1);
            }
        });
    }

    private void sleepRandom(String when) {
        try {
            long t = (long) (Math.random() * 5);
            logger.debug("({}) meet conflict sleep {} ms", when, this.instanceId, t);
            Thread.sleep(t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    private int nextProposal(int b) {
        return nextProposal(b, 10, this.config.serverId());
    }

    private static int nextProposal(int b, int m, int id) {
        return ((b / m) + 1) * m + id;
    }


    public static void accumulateMax(AtomicInteger a, int v) {
        int v0;
        do {
            v0 = a.get();
            if (v <= v0) {
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
        private final long instanceId;
        private final int proposal;
        private final ByteString value;
        private final int times;
        private volatile int totalMaxProposal = 0;
        private volatile int maxAcceptedProposal = 0;
        private volatile ByteString acceptedValue = ByteString.EMPTY;
        private IntBitSet repliedNodes = new IntBitSet();
        private volatile boolean allAccepted = true;

        public PrepareActor(long instanceId, ByteString value, int proposal, int times) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
            this.times = times;
        }

        public void begin() {
            Proposer.this.instanceId = this.instanceId;
            Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), this.instanceId, this.proposal);
            communicator.get().broadcast(req);
        }

        public synchronized void onReply(Event.PrepareResponse response) {
            logger.trace("got PREPARE reply {}", response);

            if (repliedNodes.get(response.senderId())) {
                logger.warn("Duplicated PREPARE response {}", response);
                return;
            }

            if (response.maxBallot() > this.totalMaxProposal) {
                this.totalMaxProposal = response.maxBallot();
            }

            if (response.acceptedBallot() > this.maxAcceptedProposal) {
                this.maxAcceptedProposal = response.acceptedBallot();
                this.acceptedValue = response.acceptedValue();
            }

            if (!response.success()) {
                allAccepted = false;
            }
            repliedNodes.add(response.senderId());
        }

        private boolean isAllReplied() {
            return repliedNodes.count() == config.peerCount();
        }

        private boolean isAllAccepted() {
            return this.allAccepted;
        }

        private int totalMaxProposal() {
            return this.totalMaxProposal;
        }

        private synchronized ValueWithProposal getResult() {
            logger.trace("on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                    maxAcceptedProposal, totalMaxProposal, this.proposal);

            ByteString value;
            if (maxAcceptedProposal == 0) {
                logger.trace("End prepare({}) No other chose value, max ballot is {}, use my value", this.instanceId, this.totalMaxProposal);
                value = this.value;
            }
            else {
                logger.trace("End prepare({}) use another accepted value with proposal {}", this.instanceId, maxAcceptedProposal);
                value = this.acceptedValue;
            }

            return new ValueWithProposal(this.totalMaxProposal, value);
        }
    }


    private class AcceptActor extends ActorBase {
        private final long instanceId;
        private final int proposal;
        private final ByteString value;
        private final AtomicInteger maxProposal = new AtomicInteger(0);
        private volatile boolean allAccepted = true;
        private IntBitSet repliedNodes = new IntBitSet();
        private volatile boolean chosenByOther;

        public AcceptActor(long instanceId, ByteString value, int proposal) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
            this.chosenByOther = false;
        }

        public void begin() {
            logger.debug("start accept instance {} with proposal  {} ", this.instanceId, this.proposal);
            Proposer.this.instanceId = this.instanceId;

            Event.AcceptRequest request = new Event.AcceptRequest(config.serverId(), this.instanceId, this.proposal, this.value);
            Proposer.this.communicator.get().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response) {
            if (this.repliedNodes.get(response.senderId())) {
                logger.warn("Duplicated ACCEPT response {}", response);
                return;
            }
            if (!response.accepted()) {
                this.allAccepted = false;
            }
            if (response.maxBallot() == Integer.MAX_VALUE) {
                this.chosenByOther = true;
            }

            accumulateMax(this.maxProposal, response.maxBallot());
            this.repliedNodes.add(response.senderId());
        }

        public boolean isAllAccepted() {
            return this.allAccepted;
        }

        public boolean isAllReplied() {
            return this.repliedNodes.count() == config.peerCount();
        }

        public boolean isChosenByOther() {
            return this.chosenByOther;
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

    private static class MetricsRecorder {
        private volatile long totalTimesLast = 0;
        private volatile long totalNanosLast = 0;
        private JaxosMetrics metrics;
        private AtomicInteger times = new AtomicInteger(0);

        public MetricsRecorder(JaxosMetrics metrics) {
            this.metrics = metrics;
        }

        private void recordRoundExecMetrics(long instanceId, long startNano, ProposeResult result) {
            metrics.recordPropose(System.nanoTime() - startNano, result);

            if (times.incrementAndGet() % 1000 == 0) {
                long timesDelta = metrics.proposeTimes() - this.totalTimesLast;
                double avgNanos = (metrics.proposeTotalNanos() - this.totalNanosLast) / (double) timesDelta;

                double total = metrics.proposeTimes();
                String msg = String.format("Elapsed %.3f ms(recent %d), Total %.0f s, success rate %.3f, conflict rate %.3f",
                        avgNanos / 1e+6, timesDelta,
                        total, metrics.successTimes() / total, metrics.conflictTimes() / total);
                logger.info(msg);

                this.totalTimesLast = metrics.proposeTimes();
                this.totalNanosLast = metrics.proposeTotalNanos();
            }
        }
    }
}
