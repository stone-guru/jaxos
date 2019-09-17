package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private JaxosSettings config;
    private Supplier<Communicator> communicator;
    private Learner learner;
    private final int squadId = 1;

    private HashedWheelTimer timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);

    private long instanceId = 0;
    private ByteString proposeValue;
    private PrepareActor prepareActor;
    private Timeout prepareTimeout;
    private AcceptActor acceptActor;
    private Timeout acceptTimeout;

    private CountDownLatch execEndLatch;
    private volatile ProposeResult result = null;

    private MetricsRecorder metricsRecorder;

    private AtomicInteger round = new AtomicInteger(0);

    private AtomicInteger msgId = new AtomicInteger(1);

    private ProposalNumHolder proposalNumHolder;

    public Proposer(JaxosSettings config, SquadContext context, Learner learner, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.learner = learner;
        this.metricsRecorder = new MetricsRecorder(context.jaxosMetrics());
        this.proposalNumHolder = new ProposalNumHolder(config.serverId(), 16);
    }

    public synchronized ProposeResult propose(long instanceId, ByteString value) throws InterruptedException {
        if (!communicator.get().available()) {
            return ProposeResult.NO_QUORUM;
        }

        final long startNano = System.nanoTime();
        this.proposeValue = value;

        this.result = null;
        this.execEndLatch = new CountDownLatch(1);

        startPrepare(instanceId, proposalNumHolder.getProposal0(), 1);

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

        if(logger.isTraceEnabled()) {
            logger.trace("{}: propose round for {} end with {} by {}", msgId.getAndIncrement(), this.instanceId, r.code(), reason);
        }
    }

    private boolean checkMajority(int n, String step) {
        if (n <= this.config.peerCount() / 2) {
            endWith(ProposeResult.NO_QUORUM, step);
            return false;
        }
        return true;
    }

    private void startPrepare(long instanceId, int proposal0, int times) {
        Learner.LastChosen chosen = this.learner.lastChosen();
        long next = chosen.instanceId + 1;
        if (instanceId > 0 && instanceId != next) {
            endWith(ProposeResult.conflict(this.proposeValue), "when prepare " + next + " again");
            return;
        }

        final PrepareActor actor = this.prepareActor = new PrepareActor(next, this.proposeValue, proposal0,  times, chosen.proposal);
        this.prepareTimeout = Proposer.this.timer.newTimeout(t -> this.endPrepare(actor), 1, TimeUnit.SECONDS);
        this.prepareActor.begin();
    }


    public void processPrepareResponse(Event.PrepareResponse response) {
        logger.trace("{}: RECEIVED {}", msgId.getAndIncrement(), response);

        PrepareActor actor = this.prepareActor;
        if (actor == null) {
            logger.warn("{}: Not at the state of preparing for {}", msgId.getAndIncrement(), response);
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

        if (actor.isAccepted()) {
            ValueWithProposal v = actor.getResult();
            startAccept(actor.instanceId, v.content, actor.proposal);
        }
        else {
            ValueWithProposal v = actor.getResult();
            if (v.ballot == Integer.MAX_VALUE || (actor.maxOtherChosenInstanceId >= actor.instanceId)) {
                endWith(ProposeResult.conflict(this.proposeValue), "CONFLICT other value chosen");
                return;
            }

            if(actor.times > 3) {
                endWith(ProposeResult.conflict(this.proposeValue), "PREPARE conflict 3 times");
                return;
            }

            if (actor.times >= 1) {
                sleepRandom(actor.times, "PREPARE");
            }

            startPrepare(actor.instanceId, proposalNumHolder.nextProposal(actor.totalMaxProposal), actor.times + 1);
        }
    }

    private void startAccept(long instanceId, ByteString value, int proposal) {
        this.acceptActor = new AcceptActor(instanceId, value, proposal);
        this.acceptTimeout = this.timer.newTimeout(t -> endAccept(), 1, TimeUnit.SECONDS);
        this.acceptActor.begin();
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        logger.trace("{}: RECEIVED {}", msgId.getAndIncrement(), response);

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
        logger.trace("{}: End Accept", msgId.getAndIncrement());
        this.acceptTimeout.cancel();
        AcceptActor actor = this.acceptActor;
        if (actor == null) {
            return; //already executed
        }
        actor.once(() -> {
            if (!checkMajority(actor.repliedNodes.count(), "ACCEPT")) {
                return;
            }

            if (actor.isAccepted()) {
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

    private void sleepRandom(int i, String when) {
        try {
            long t = (long) (Math.random() * 10 * i);
            logger.debug("{}: ({}) meet conflict sleep {} ms", msgId.getAndIncrement(), when, this.instanceId, t);
            Thread.sleep(t);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
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
        private final int chosenProposal;

        private int totalMaxProposal = 0;
        private int maxAcceptedProposal = 0;
        private ByteString acceptedValue = ByteString.EMPTY;
        private final IntBitSet repliedNodes = new IntBitSet();

        private boolean someOneReject = false;
        private int acceptedCount = 0;
        private long maxOtherChosenInstanceId = 0;

        public PrepareActor(long instanceId, ByteString value, int proposal, int times, int chosenProposal) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
            this.times = times;
            this.chosenProposal = chosenProposal;
        }

        public void begin() {
            Proposer.this.instanceId = this.instanceId;
            Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), squadId, this.instanceId, this.times, this.proposal, this.chosenProposal);
            communicator.get().broadcast(req);
        }

        public synchronized void onReply(Event.PrepareResponse response) {
            logger.trace("{}: On PREPARE reply {}", msgId.getAndIncrement(), response);

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

            switch (response.result()){
                case Event.RESULT_SUCCESS: {
                    acceptedCount++;
                    break;
                }
                case Event.RESULT_REJECT: {
                    someOneReject = true;
                    break;
                }
            }

            if(response.chosenInstanceId() >= this.maxOtherChosenInstanceId){
                this.maxOtherChosenInstanceId = response.chosenInstanceId();
            }

            repliedNodes.add(response.senderId());
        }

        private boolean isAllReplied() {
            return repliedNodes.count() == config.peerCount();
        }

        private boolean isAccepted() {
            return !this.someOneReject && acceptedCount > config.peerCount()/2;
        }

        private int totalMaxProposal() {
            return this.totalMaxProposal;
        }

        public long maxOtherChosenInstanceId(){
            return this.maxOtherChosenInstanceId;
        }

        private synchronized ValueWithProposal getResult() {
            logger.trace("{}: on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                    msgId.getAndIncrement(), maxAcceptedProposal, totalMaxProposal, this.proposal);

            ByteString value;
            if (maxAcceptedProposal == 0) {
                logger.trace("{}: End prepare({}) No other chose value, max ballot is {}, use my value", msgId.getAndIncrement(), this.instanceId, this.totalMaxProposal);
                value = this.value;
            }
            else {
                logger.trace("{}: End prepare({}) use another accepted value with proposal {}", msgId.getAndIncrement(), this.instanceId, maxAcceptedProposal);
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
        private int acceptedCount = 0;
        private volatile boolean chosenByOther;

        public AcceptActor(long instanceId, ByteString value, int proposal) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
            this.chosenByOther = false;
        }

        public void begin() {
            logger.debug("{}: start accept instance {} with proposal  {} ", msgId.getAndIncrement(), this.instanceId, this.proposal);
            Proposer.this.instanceId = this.instanceId;

            Event.AcceptRequest request = new Event.AcceptRequest(config.serverId(), squadId, this.instanceId, round.get(), this.proposal, this.value);
            Proposer.this.communicator.get().broadcast(request);
        }

        public synchronized void onReply(Event.AcceptResponse response) {
            if (this.repliedNodes.get(response.senderId())) {
                logger.warn("{}: Duplicated ACCEPT response {}", msgId.getAndIncrement(), response);
                return;
            }
            if (response.result() == Event.RESULT_REJECT) {
                this.allAccepted = false;
            } else if (response.result() == Event.RESULT_SUCCESS){
                this.acceptedCount++;
            }

            if (response.maxBallot() == Integer.MAX_VALUE) {
                this.chosenByOther = true;
            }

            accumulateMax(this.maxProposal, response.maxBallot());
            this.repliedNodes.add(response.senderId());
        }

        public boolean isAccepted() {
            return this.allAccepted && config.reachQuorum(acceptedCount);
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
            logger.debug("{}: Notify instance {} chosen", msgId.getAndIncrement(), this.instanceId);

            //Then notify other peers
            Event notify = new Event.ChosenNotify(config.serverId(), squadId, this.instanceId, this.proposal);
            communicator.get().selfFirstBroadcast(notify);
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
