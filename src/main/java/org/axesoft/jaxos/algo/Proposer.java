package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class Proposer {
    private static final Logger logger = LoggerFactory.getLogger(Proposer.class);

    public enum Stage {
        NONE, PREPARING, ACCEPTING
    }

    private final JaxosSettings settings;
    private final SquadContext context;
    private final Configuration config;
    private final Learner learner;
    private final ProposalNumHolder proposalNumHolder;

    private PrepareActor prepareActor;
    private Timeout prepareTimeout;
    private AcceptActor acceptActor;
    private Timeout acceptTimeout;

    private ByteString proposeValue;
    private long instanceId = 0;
    private int messageMark = 0;
    private int round = 0;
    private Stage stage;
    private CountDownLatch execEndLatch;
    private volatile ProposeResult result;

    public Proposer(JaxosSettings settings, Configuration config, SquadContext context, Learner learner) {
        this.settings = settings;
        this.config = config;
        this.context = context;
        this.learner = learner;
        this.proposalNumHolder = new ProposalNumHolder(this.settings.serverId(), JaxosSettings.SERVER_ID_RANGE);
        this.stage = Stage.NONE;
        this.prepareActor = new PrepareActor();
        this.acceptActor = new AcceptActor();
    }

    public synchronized ProposeResult propose(long instanceId, ByteString value) throws InterruptedException {
        if (!config.getCommunicator().available()) {
            return ProposeResult.NO_QUORUM;
        }

        this.instanceId = instanceId;
        this.proposeValue = value;
        this.round = 0;
        this.result = null;

        this.execEndLatch = new CountDownLatch(1);

        if (settings.ignoreLeader()) {
            startPrepare(proposalNumHolder.getProposal0());
        }
        else if (context.isOtherLeaderActive()) {
            return ProposeResult.otherLeader(context.lastSuccessPrepare().serverId());
        }
        else if (context.isLeader()) {
            startAccept(this.proposeValue, context.lastSuccessPrepare().proposal());
        }
        else {
            startPrepare(proposalNumHolder.getProposal0());
        }

        this.execEndLatch.await(this.settings.wholeProposalTimeoutMillis(), TimeUnit.MILLISECONDS);
        if (this.result == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Whole Propose timeout");
            }
            this.result = ProposeResult.TIME_OUT;
        }

        return this.result;
    }

    private void endWith(ProposeResult r, String reason) {
        this.result = r;
        this.stage = Stage.NONE;
        this.execEndLatch.countDown();

        if (logger.isTraceEnabled()) {
            logger.trace("Propose instance({}) end with {} by {}", this.instanceId, r.code(), reason);
        }
    }

    private boolean endWithMajorityCheck(int n, String step) {
        if (n <= this.settings.peerCount() / 2) {
            endWith(ProposeResult.NO_QUORUM, step);
            return true;
        }
        return false;
    }

    private void startPrepare(int proposal0) {
        Learner.LastChosen chosen = this.learner.lastChosen(this.context.squadId());
        if (instanceId != chosen.instanceId + 1) {
            endWith(ProposeResult.conflict(this.proposeValue),
                    String.format("when prepare instance %d while last chosen is %d", instanceId, chosen.instanceId));
            return;
        }

        this.stage = Stage.PREPARING;
        this.round++;
        this.messageMark++;
        this.prepareActor.startNewRound(this.proposeValue, proposal0, chosen.proposal);

        this.prepareTimeout = config.getEventTimer().createTimeout(this.settings.prepareTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.PrepareTimeout(this.settings.serverId(), this.context.squadId(), this.instanceId, this.messageMark));
    }


    public void onPrepareReply(Event.PrepareResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("RECEIVED {}", response);
        }

        if (eventMatchRequest(response, Stage.PREPARING)) {
            this.prepareActor.onReply(response);
            if (this.prepareActor.isAllReplied()) {
                this.prepareTimeout.cancel();
                endPrepare();
            }
        }
    }

    public void onPrepareTimeout(Event.PrepareTimeout event) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", event);
        }

        if (eventMatchRequest(event, Stage.PREPARING)) {
            endPrepare();
        }
    }

    private void endPrepare() {
        if (endWithMajorityCheck(this.prepareActor.votedCount(), "PREPARE")) {
            return;
        }

        ValueWithProposal v = this.prepareActor.getResult();
        if (this.prepareActor.isAccepted()) {
            startAccept(v.content, this.prepareActor.myProposal());
        }
        else {
            if (this.prepareActor.totalMaxProposal == Integer.MAX_VALUE
                    || this.prepareActor.maxOtherChosenInstanceId() >= this.instanceId) {
                endWith(ProposeResult.conflict(this.proposeValue), "CONFLICT other value chosen");
                return;
            }

            if (this.round > 3) {
                endWith(ProposeResult.conflict(this.proposeValue), "PREPARE conflict 3 times");
                return;
            }

            if (this.round >= 1) {
                //sleepRandom(actor.times, "PREPARE");
            }

            startPrepare(proposalNumHolder.nextProposal(this.prepareActor.totalMaxProposal()));
        }
    }

    private void startAccept(ByteString value, int proposal) {
        Learner.LastChosen chosen = this.learner.lastChosen(this.context.squadId());
        if (instanceId != chosen.instanceId + 1) {
            endWith(ProposeResult.conflict(this.proposeValue),
                    String.format("when accept instance %d while last chosen is %d", instanceId, chosen.instanceId));
            return;
        }
        this.stage = Stage.ACCEPTING;
        this.acceptActor.startAccept(value, proposal, chosen.proposal);

        this.acceptTimeout = config.getEventTimer().createTimeout(this.settings.acceptTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.AcceptTimeout(settings.serverId(), this.context.squadId(), this.instanceId, this.messageMark));
    }

    public void onAcceptReply(Event.AcceptResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("RECEIVED {}", response);
        }

        if (eventMatchRequest(response, Stage.ACCEPTING)) {
            this.acceptActor.onReply(response);
            if (this.acceptActor.isAllReplied()) {
                this.acceptTimeout.cancel();
                endAccept();
            }
        }
    }

    public void onAcceptTimeout(Event.AcceptTimeout event) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", event);
        }
        if (eventMatchRequest(event, Stage.ACCEPTING)) {
            endAccept();
        }
    }

    private void endAccept() {
        if (logger.isTraceEnabled()) {
            logger.trace("Process End Accept");
        }

        if (endWithMajorityCheck(this.acceptActor.votedCount(), "ACCEPT")) {
            return;
        }

        if (this.acceptActor.isAccepted()) {
            this.acceptActor.notifyChosen();
            if (this.acceptActor.sentValue() == this.proposeValue) {
                endWith(ProposeResult.success(this.instanceId), "Chosen");
            }
            else {
                endWith(ProposeResult.conflict(this.proposeValue), "Accept send other value");
            }
        }
        else if (this.acceptActor.isChosenByOther()) {
            endWith(ProposeResult.conflict(this.proposeValue), "Chosen by other at accept");
        }
        else if (this.round >= 2) {
            endWith(ProposeResult.conflict(this.proposeValue), "REJECT at accept");
        }
        else {
            //use another prepare status to detect, whether this value chosen
            //FIXME handle case of someone reject, or self message delay or repeated
            startPrepare(this.proposalNumHolder.getProposal0());
        }
    }

    private void sleepRandom(int i, String when) {
        try {
            long t = (long) (Math.random() * 10 * i);
            logger.debug("({}) meet conflict sleep {} ms", when, this.instanceId, t);
            Thread.sleep(t);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    private boolean eventMatchRequest(Event.BallotEvent event, Stage expectedStage) {
        if (this.stage != expectedStage) {
            logger.debug("Not at stage of PREPARING on ", event);
            return false;
        }
        if (event.instanceId() != this.instanceId) {
            logger.debug("Instance of {] not equal to mine {}", event, this.instanceId);
            return false;
        }
        if (event.round() != this.messageMark) {
            logger.debug("Round of {] not equal to mine {}", event, this.round);
            return false;
        }
        return true;
    }

    private class PrepareActor {
        private int proposal;
        private ByteString value;
        private int chosenProposal;

        private int totalMaxProposal = 0;
        private int maxAcceptedProposal = 0;
        private ByteString acceptedValue = ByteString.EMPTY;
        private final IntBitSet repliedNodes = new IntBitSet();

        private boolean someOneReject = false;
        private int acceptedCount = 0;
        private long maxOtherChosenInstanceId = 0;
        private int votedCount = 0;

        public PrepareActor() {
        }

        public void startNewRound(ByteString value, int proposal, int chosenProposal) {
            //reset accumulated values
            this.totalMaxProposal = 0;
            this.maxAcceptedProposal = 0;
            this.acceptedValue = ByteString.EMPTY;
            this.repliedNodes.clear();

            //init values for this round
            this.value = value;
            this.proposal = proposal;
            this.chosenProposal = chosenProposal;

            Event.PrepareRequest req = new Event.PrepareRequest(
                    Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, Proposer.this.messageMark,
                    this.proposal, this.chosenProposal);

            config.getCommunicator().broadcast(req);
        }

        public void onReply(Event.PrepareResponse response) {
            if (logger.isTraceEnabled()) {
                logger.trace("On PREPARE reply {}", response);
            }
            repliedNodes.add(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("On PrepareReply: Server {} is standby at last chosen instance id is {}",
                            response.senderId(), response.chosenInstanceId());
                }
                return;
            }

            votedCount++;
            if (response.result() == Event.RESULT_SUCCESS) {
                acceptedCount++;
            }
            else if (response.result() == Event.RESULT_SUCCESS) {
                someOneReject = true;
            }

            if (response.maxBallot() > this.totalMaxProposal) {
                this.totalMaxProposal = response.maxBallot();
            }

            if (response.acceptedBallot() > this.maxAcceptedProposal) {
                this.maxAcceptedProposal = response.acceptedBallot();
                this.acceptedValue = response.acceptedValue();
            }

            if (response.chosenInstanceId() >= this.maxOtherChosenInstanceId) {
                this.maxOtherChosenInstanceId = response.chosenInstanceId();
            }
        }

        private boolean isAllReplied() {
            return repliedNodes.count() == settings.peerCount();
        }

        private boolean isAccepted() {
            return !this.someOneReject && acceptedCount > settings.peerCount() / 2;
        }

        private int myProposal() {
            return this.proposal;
        }

        private int votedCount() {
            return this.votedCount;
        }

        private int totalMaxProposal() {
            return this.totalMaxProposal;
        }

        public long maxOtherChosenInstanceId() {
            return this.maxOtherChosenInstanceId;
        }

        private ValueWithProposal getResult() {
            if (logger.isTraceEnabled()) {
                logger.trace("on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                        maxAcceptedProposal, totalMaxProposal, this.proposal);
            }

            ByteString value;
            if (this.maxAcceptedProposal == 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Result of prepare({}) No other chose value, total max ballot is {}, use my value",
                            Proposer.this.instanceId, this.totalMaxProposal);
                }
                value = this.value;
            }
            else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Result of prepare({}), other accepted value with proposal {}",
                            Proposer.this.instanceId, maxAcceptedProposal);
                }
                value = this.acceptedValue;
            }

            return new ValueWithProposal(this.totalMaxProposal, value);
        }
    }

    private class AcceptActor {
        private int maxProposal = 0;
        private boolean someoneReject = true;
        private IntBitSet repliedNodes = new IntBitSet();
        private int acceptedCount = 0;
        private boolean chosenByOther;
        private int proposal;
        private int votedCount = 0;
        private ByteString sentValue;

        public void startAccept(ByteString value, int proposal, int lastChosenProposal) {
            this.maxProposal = 0;
            this.someoneReject = false;
            this.repliedNodes.clear();
            this.acceptedCount = 0;
            this.chosenByOther = false;
            this.votedCount = 0;
            this.proposal = proposal;
            this.sentValue = value;

            logger.debug("Start accept instance {} with proposal  {} ", Proposer.this.instanceId, proposal);

            Event.AcceptRequest request = new Event.AcceptRequest(
                    Proposer.this.settings.serverId(), Proposer.this.context.squadId(), Proposer.this.instanceId, Proposer.this.messageMark,
                    proposal, value, lastChosenProposal);
            Proposer.this.config.getCommunicator().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response) {
            this.repliedNodes.add(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("AcceptReply: Server {} is standby at last chosen instance id is {}",
                            response.senderId(), response.chosenInstanceId());
                }
                return;
            }

            votedCount++;
            if (response.result() == Event.RESULT_REJECT) {
                this.someoneReject = true;
            }
            else if (response.result() == Event.RESULT_SUCCESS) {
                this.acceptedCount++;
            }

            if (response.maxBallot() == Integer.MAX_VALUE) {
                this.chosenByOther = true;
            }

            if (response.maxBallot() > this.maxProposal) {
                this.maxProposal = response.maxBallot();
            }
        }

        boolean isAccepted() {
            return !this.someoneReject && settings.reachQuorum(acceptedCount);
        }

        boolean isAllReplied() {
            return this.repliedNodes.count() == settings.peerCount();
        }

        boolean isChosenByOther() {
            return this.chosenByOther;
        }

        int votedCount() {
            return this.votedCount;
        }

        ByteString sentValue() {
            return this.sentValue;
        }

        void notifyChosen() {
            logger.debug("Notify instance {} chosen", Proposer.this.instanceId);

            //Then notify other peers
            Event notify = new Event.ChosenNotify(Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, this.proposal);
            Proposer.this.config.getCommunicator().selfFirstBroadcast(notify);
        }
    }
}
