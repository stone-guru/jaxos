package org.jaxos.algo;

import com.google.protobuf.ByteString;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.jaxos.JaxosConfig;
import org.jaxos.base.EntryOnce;
import org.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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
    private volatile ByteString proposeValue;

    private HashedWheelTimer timer = new HashedWheelTimer(3, TimeUnit.SECONDS);
    private Timeout timeout;

    private volatile PrepareActor prepareActor = null;
    private EntryOnce preparedEntry = new EntryOnce();

    private volatile AcceptActor acceptActor = null;
    private EntryOnce acceptedEntry = new EntryOnce();

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
        askPrepare(this.config.serverId());

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

            logger.info("{} total propose times is {}, average elapsed {} ms in recent {} times",
                    new Date(), m.proposeTimes(), avgNanos / 1e+6, timesDelta);

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
        this.prepareActor = new PrepareActor(this.instanceContext.lastInstanceId() + 1, this.proposeValue, ballot0);
        this.prepareActor.begin();
    }


    public void onPrepareReply(Event.PrepareResponse response) {
        logger.debug("RECEIVED {}", response);

        PrepareActor actor = this.prepareActor;
        if(actor == null){
            logger.warn("Not at state of preparing for {}", response);
            return;
        }

        actor.onReply(response);
        if(actor.isAllReplied()){
            preparedEntry.exec(this::startAccept);
        }
    }

    private void startAccept(){
        PrepareActor actor = this.prepareActor;
        ValueWithProposal v = actor.getResult();
        this.acceptActor = new AcceptActor(actor.instanceId, v.content, v.ballot);
        this.acceptActor.begin();

        this.prepareActor = null;
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        AcceptActor actor = this.acceptActor;
        if(actor == null){
            logger.warn("Not at state of accepting for {}", response);
            return;
        }

        actor.onReply(response);
        if(actor.isAllReplied()){
            acceptedEntry.exec(() -> {
                actor.notifyChosen();
                endWith(ProposeResult.SUCCESS);
                //FIXME askPrepare(nextBallot(this.totalMaxBallot.get()));
            });
        }
    }

    private int nextBallot(int b) {
        return nextBallot(b, 10, this.config.serverId());
    }

    private static int nextBallot(int b, int m, int id) {
        return ((b / m) + 1) * m + id;
    }

    private boolean checkMajority(int n) {
        if (n <= this.config.peerCount() / 2) {
            endWith(ProposeResult.NO_QUORUM);
            return false;
        }
        return true;
    }


    private class PrepareActor {
        private long instanceId;
        private int proposal;
        private ByteString value;
        private AtomicInteger totalMaxBallot = new AtomicInteger(0);
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

        public void begin(){
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
                accumulateTotalMaxBallot(response.maxBallot());
                if (response.acceptedBallot() > this.maxAcceptedValue.get().ballot) {
                    accumulateAcceptedValue(response.acceptedBallot(), response.acceptedValue());
                }
            }

            if(!response.success()){
                allAccepted = false;
            }
        }

        public boolean isAllReplied(){
            return repliedNodes.count() == config.peerCount();
        }

        public boolean isAllAccepted(){
            return this.allAccepted;
        }

        private ValueWithProposal getResult(){
            ValueWithProposal mv = this.maxAcceptedValue.get();

            logger.debug("on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                    this.maxAcceptedValue.get().ballot, mv.ballot, this.proposal);

            int maxiBallot = this.totalMaxBallot.get();
            int b0 = this.proposal;
            int newBallot = maxiBallot > b0 ? nextBallot(maxiBallot) : b0;

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
            ValueWithProposal v0, v1;
            do {
                v0 = this.maxAcceptedValue.get();
                if (v0.ballot >= ballot) {
                    return;
                }
                v1 = new ValueWithProposal(ballot, value);
                logger.info("There are another accepted value \"{}\"", value.toStringUtf8()); //FIXME not string in future
            } while (!this.maxAcceptedValue.compareAndSet(v0, v1));
        }
    }


    private class AcceptActor {
        private final long instanceId;
        private final int proposal;
        private final ByteString value;
        private volatile boolean allAccepted;
        private IntBitSet repliedNodes = new IntBitSet();

        public AcceptActor(long instanceId, ByteString value, int proposal) {
            this.instanceId = instanceId;
            this.value = value;
            this.proposal = proposal;
        }

        public void begin(){
            logger.debug("new ballot is {} ", proposal);
            Event.AcceptRequest request = new Event.AcceptRequest(config.serverId(), this.instanceId, this.proposal, this.value);
            Proposer.this.communicator.get().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response){
            if (this.repliedNodes.get(response.senderId())) {
                logger.warn("Duplicated ACCEPT response {}", response);
                return;
            }
            repliedNodes.add(response.senderId());
            if (!response.accepted()) {
                this.allAccepted = false;
            }
        }

        public boolean isAllAccepted(){
            return this.allAccepted;
        }

        public boolean isAllReplied(){
            return this.repliedNodes.count() == config.peerCount();
        }

        public void notifyChosen(){
            logger.debug("Notify instance {} chosen", this.instanceId);
            Event notify = new Event.ChosenNotify(config.serverId(), this.instanceId, this.proposal);
            communicator.get().broadcast(notify);
        }
    }

}
