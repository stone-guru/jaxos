package org.axesoft.jaxos.algo;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The standard paxos proposer
 *
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
    private final Components config;
    private final Learner learner;
    private final ProposalNumHolder proposalNumHolder;
    private final BallotIdHolder ballotIdHolder;

    private Timeout proposalTimeout;
    private PrepareActor prepareActor;
    private Timeout prepareTimeout;
    private AcceptActor acceptActor;
    private Timeout acceptTimeout;

    private Event.BallotValue proposeValue;
    private long instanceId = 0;
    private long ballotId = 0;
    private int round = 0;
    private Stage stage;

    private BitSet allNodeIds;
    private BitSet failingNodeIds;
    private int answerNodeCount;

    private AtomicReference<SettableFuture<Void>> resultFutureRef;

    public Proposer(JaxosSettings settings, Components config, SquadContext context, Learner learner) {
        this.settings = settings;
        this.config = config;
        this.context = context;
        this.learner = learner;
        this.proposalNumHolder = new ProposalNumHolder(this.settings.serverId(), JaxosSettings.SERVER_ID_RANGE);
        this.ballotIdHolder = new BallotIdHolder(this.settings.serverId());

        this.stage = Stage.NONE;
        this.prepareActor = new PrepareActor();
        this.acceptActor = new AcceptActor();

        this.failingNodeIds = new BitSet();
        this.allNodeIds = new BitSet();
        for (int id : this.settings.peerMap().keySet()) {
            this.allNodeIds.set(id);
        }

        this.resultFutureRef = new AtomicReference<>(null);
    }

    public ListenableFuture<Void> propose(long instanceId, Event.BallotValue value, boolean ignoreLeader, SettableFuture<Void> resultFuture) {
        if (!resultFutureRef.compareAndSet(null, resultFuture)) {
            resultFuture.setException(new ConcurrentModificationException("Previous propose not end"));
            return resultFuture;
        }

        if (!config.getCommunicator().available()) {
            return endAs(new CommunicatorException("Not enough server connected"));
        }

        this.instanceId = instanceId;
        this.proposeValue = value;
        this.round = 0;
        this.ballotId = this.ballotIdHolder.nextId();

        if (settings.leaderless()) {
            startPrepare(proposalNumHolder.getProposal0());
        }
        else if (context.isOtherLeaderActive() && !ignoreLeader) {
            return endAs(new RedirectException(context.lastSuccessAccept().serverId()));
        }
        else if (context.isLeader()) {
            startAccept(this.proposeValue, context.lastSuccessAccept().proposal(), settings.serverId());
        }
        else {
            startPrepare(proposalNumHolder.getProposal0());
        }

        this.proposalTimeout = config.getEventTimer().createTimeout(this.settings.wholeProposalTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.ProposalTimeout(this.settings.serverId(), this.context.squadId(), this.instanceId, 0));
        return resultFuture;
    }

    private ListenableFuture<Void> endAs(Throwable error) {
        this.stage = Stage.NONE;
        if (this.proposalTimeout != null) {
            this.proposalTimeout.cancel();
            this.proposalTimeout = null;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("S{}: Propose instance({}) end with {}",
                    context.squadId(), this.instanceId, error == null ? "SUCCESS" : error.getMessage());
        }
        SettableFuture<Void> future = this.resultFutureRef.get();
        this.resultFutureRef.set(null);

        if (error == null) {
            future.set(null);
        }
        else {
            future.setException(error);
        }
        return future;
    }

    private boolean endWithMajorityCheck(int n, String step) {
        if (n <= this.settings.peerCount() / 2) {
            endAs(new NoQuorumException("Not enough peers response at " + step));
            return true;
        }
        return false;
    }

    public void onProposalTimeout(Event.ProposalTimeout event) {
        if (this.stage != Stage.NONE && event.squadId() == context.squadId() && event.instanceId() == this.instanceId) {
            endAs(new TimeoutException(String.format("S%d I%d whole proposal timeout", context.squadId(), this.instanceId)));
        }
        else {
            logger.info("Ignore unnecessary {}", event);
        }
    }

    private void startPrepare(int proposal0) {
        Learner.LastChosen chosen = this.learner.lastChosen(this.context.squadId());
        if (this.instanceId != chosen.instanceId + 1) {
            String msg = String.format("when prepare instance %d while last chosen is %d.%d",
                    context.squadId(), instanceId, chosen.instanceId);
            endAs(new ProposalConflictException(msg));
            return;
        }

        this.stage = Stage.PREPARING;
        this.round++;
        this.prepareActor.startNewRound(this.proposeValue, proposal0, chosen.proposal);

        this.prepareTimeout = config.getEventTimer().createTimeout(this.settings.prepareTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.PrepareTimeout(this.settings.serverId(), this.context.squadId(), this.instanceId, this.round));
    }


    public void onPrepareReply(Event.PrepareResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("RECEIVED {}", response);
        }
        //As long as a server give back a response, it's no longer a failing node
        this.failingNodeIds.clear(response.senderId());

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
        recordAnswerNodes(this.prepareActor.repliedNodes);

        if (endWithMajorityCheck(this.prepareActor.votedCount(), "PREPARE " + instanceId)) {
            return;
        }

        PrepareResult v = this.prepareActor.getResult();
        if (this.prepareActor.isApproved()) {
            if (this.prepareActor.maxAcceptedProposal > 0) {
                startAccept(this.prepareActor.acceptedValue, this.prepareActor.myProposal(), this.prepareActor.ballotId);
            }
            else if (this.prepareActor.maxAcceptedProposal == 0) {
                startAccept(this.proposeValue, this.prepareActor.myProposal(), this.ballotId);
            }
            else {
                throw new IllegalStateException("Got negative proposal id");
            }
        }
        else {
            if (this.prepareActor.totalMaxProposal == Integer.MAX_VALUE) {
                if (v.ballotId == this.ballotId) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("S{} I{} Other proposer help finish ", context.squadId(), this.instanceId);
                    }
                    endAs(null);
                }
                else {
                    endAs(new ProposalConflictException(this.instanceId + " CONFLICT other value chosen"));
                }
                return;
            }
            sleepRandom(round, "PREPARE " + context.squadId() + "." + this.instanceId);
            startPrepare(proposalNumHolder.proposalGreatThan(this.prepareActor.totalMaxProposal()));
        }
    }


    private void recordAnswerNodes(BitSet answerNodes) {
        if (this.answerNodeCount != answerNodes.cardinality()) {
            logger.info("S {} Answer node changed from {} to {}",
                    this.context.squadId(), this.answerNodeCount, answerNodes.cardinality());
        }

        this.answerNodeCount = answerNodes.cardinality();
        this.failingNodeIds = (BitSet) this.allNodeIds.clone();
        this.failingNodeIds.andNot(answerNodes);
    }

    private void startAccept(Event.BallotValue value, int proposal, long ballotId) {
        Learner.LastChosen chosen = this.learner.lastChosen(this.context.squadId());
        if (instanceId != chosen.instanceId + 1) {
            String msg = String.format("when accept instance %d.%d while last chosen is %d",
                    context.squadId(), instanceId, chosen.instanceId);
            endAs(new ProposalConflictException(msg));
            return;
        }
        this.stage = Stage.ACCEPTING;
        this.acceptActor.startAccept(value, proposal, ballotId, chosen.proposal);

        this.acceptTimeout = config.getEventTimer().createTimeout(this.settings.acceptTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.AcceptTimeout(settings.serverId(), this.context.squadId(), this.instanceId, this.round));
    }

    void onAcceptReply(Event.AcceptResponse response) {
        if (logger.isTraceEnabled()) {
            logger.trace("RECEIVED {}", response);
        }

        //As long as a server give back a response, it's no longer a failing node
        this.failingNodeIds.clear(response.senderId());

        if (eventMatchRequest(response, Stage.ACCEPTING)) {
            this.acceptActor.onReply(response);
            if (this.acceptActor.isAllReplied()) {
                this.acceptTimeout.cancel();
                endAccept();
            }
        }
    }

    void onAcceptTimeout(Event.AcceptTimeout event) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", event);
        }
        if (eventMatchRequest(event, Stage.ACCEPTING)) {
            endAccept();
        }
    }

    private void endAccept() {
        if (logger.isTraceEnabled()) {
            logger.trace("S{} Process End Accept({})", context.squadId(), this.instanceId);
        }

        recordAnswerNodes(this.acceptActor.repliedNodes);

        if (endWithMajorityCheck(this.acceptActor.votedCount(), "ACCEPT")) {
            return;
        }

        if (this.acceptActor.isAccepted()) {
            this.acceptActor.notifyChosen();
            if (this.acceptActor.ballotId == this.ballotId) {
                endAs(null);
            }
            else {
                String msg = String.format("S%d I%d send other value at Accept", context.squadId(), this.instanceId);
                endAs(new ProposalConflictException(msg));
            }
        }
        else if (this.acceptActor.isInstanceChosen()) {
            endAs(new ProposalConflictException("Chosen by other at accept"));
        }
        else if (this.round >= 100) {
            endAs(new ProposalConflictException("REJECT at accept more than 100 times"));
        }
        else {
            //use another prepare status to detect, whether this value chosen
            //FIXME handle case of someone reject, or self message delay or repeated
            startPrepare(this.proposalNumHolder.proposalGreatThan(acceptActor.maxProposal));
        }
    }

    private void sleepRandom(int i, String when) {
        try {
            long t = Math.max(1L, (long) (Math.random() * 50));
            if (logger.isDebugEnabled()) {
                logger.debug("({} {}.{}) meet conflict sleep {} ms", when, context.squadId(), this.instanceId, t);
            }
            Thread.sleep(t);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }


    private boolean eventMatchRequest(Event.BallotEvent event, Stage expectedStage) {
        if (this.stage != expectedStage) {
            logger.debug("Not at stage of PREPARING on {}", event);
            return false;
        }
        if (event.instanceId() != this.instanceId) {
            logger.debug("Instance of {} not equal to mine {}", event, this.instanceId);
            return false;
        }
        if (event.round() != this.round) {
            logger.debug("Round of {} not equal to mine {}", event, this.round);
            return false;
        }
        return true;
    }

    private class PrepareActor {
        private int proposal;
        private Event.BallotValue value;

        private int totalMaxProposal = 0;
        private int maxAcceptedProposal = 0;
        private Event.BallotValue acceptedValue;
        private final BitSet repliedNodes = new BitSet();
        private long ballotId = 0;

        private boolean someOneReject = false;
        private int approvedCount = 0;
        private long maxOtherChosenInstanceId = 0;
        private int votedCount = 0;

        public PrepareActor() {
        }

        public void startNewRound(Event.BallotValue value, int proposal, int chosenProposal) {
            //reset accumulated values
            this.totalMaxProposal = 0;
            this.maxAcceptedProposal = 0;
            this.acceptedValue = null;
            this.repliedNodes.clear();

            //init values for this round
            this.value = value;
            this.proposal = proposal;
            //this.valueProposer = valueProposer;

            Event.PrepareRequest req = new Event.PrepareRequest(
                    Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, Proposer.this.round,
                    this.proposal, chosenProposal);

            config.getCommunicator().broadcast(req);
        }

        public void onReply(Event.PrepareResponse response) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{}: On PREPARE reply {}", context.squadId(), response);
            }
            if (repliedNodes.get(response.senderId())) {
                logger.warn("Abandon duplicated response {}", response);
                return;
            }

            repliedNodes.set(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{}: On PrepareReply: Server {} is standby at last chosen instance id is {}",
                            context.squadId(), response.senderId(), response.chosenInstanceId());
                }
                return;
            }

            votedCount++;
            if (response.result() == Event.RESULT_SUCCESS) {
                approvedCount++;
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
                this.ballotId = response.ballotId();
            }

            if (response.chosenInstanceId() >= this.maxOtherChosenInstanceId) {
                this.maxOtherChosenInstanceId = response.chosenInstanceId();
            }
        }

        private boolean isAllReplied() {
            return repliedNodes.cardinality() >= settings.peerCount() - failingNodeIds.cardinality();
        }

        private boolean isApproved() {
            return !this.someOneReject && approvedCount > settings.peerCount() / 2;
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

        private int maxAcceptedProposal() {
            return this.maxAcceptedProposal;
        }

        private long maxOtherChosenInstanceId() {
            return this.maxOtherChosenInstanceId;
        }

        private PrepareResult getResult() {
            if (logger.isTraceEnabled()) {
                logger.trace("S{} Instance {} on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                        context.squadId(), instanceId, maxAcceptedProposal, totalMaxProposal, this.proposal);
            }

            if (this.maxAcceptedProposal == 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("S{} Result of prepare({}) No other chose value, total max ballot is {}",
                            context.squadId(), Proposer.this.instanceId, this.totalMaxProposal);
                }
                return PrepareResult.of(0, this.value, 0);
            }
            else {
                if (logger.isTraceEnabled()) {
                    logger.trace("S{}: Result of prepare({}), other accepted value with proposal {}",
                            context.squadId(), Proposer.this.instanceId, maxAcceptedProposal);
                }
                return PrepareResult.of(this.totalMaxProposal, this.acceptedValue, this.ballotId);
            }
        }
    }

    private class AcceptActor {
        private Event.BallotValue sentValue;
        private int proposal;
        private long ballotId;

        private int maxProposal = 0;
        private boolean someoneReject = true;
        private BitSet repliedNodes = new BitSet();
        private int acceptedCount = 0;
        private boolean instanceChosen;
        private int votedCount = 0;


        public void startAccept(Event.BallotValue value, int proposal, long ballotId, int lastChosenProposal) {
            this.proposal = proposal;
            this.sentValue = value;
            this.ballotId = ballotId;
            this.maxProposal = 0;
            this.someoneReject = false;
            this.repliedNodes.clear();
            this.acceptedCount = 0;
            this.instanceChosen = false;
            this.votedCount = 0;

            if (logger.isDebugEnabled()) {
                logger.debug("S{}: Start accept instance {} with proposal  {} ", context.squadId(), Proposer.this.instanceId, proposal);
            }

            Event.AcceptRequest request = Event.AcceptRequest.newBuilder(Proposer.this.settings.serverId(),
                    Proposer.this.context.squadId(), Proposer.this.instanceId, Proposer.this.round)
                    .setBallot(proposal)
                    .setValue(value)
                    .setBallotId(ballotId)
                    .setLastChosenBallot(lastChosenProposal)
                    .build();
            Proposer.this.config.getCommunicator().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response) {
            this.repliedNodes.set(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{} AcceptReply: Server {} is standby at last chosen instance id is {}",
                            context.squadId(), response.senderId(), response.chosenInstanceId());
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
                this.instanceChosen = true;
            }

            if (response.maxBallot() > this.maxProposal) {
                this.maxProposal = response.maxBallot();
            }
        }

        boolean isAccepted() {
            return !this.someoneReject && settings.reachQuorum(acceptedCount);
        }

        boolean isAllReplied() {
            return this.repliedNodes.cardinality() >= settings.peerCount() - failingNodeIds.cardinality();
        }

        boolean isInstanceChosen() {
            return this.instanceChosen;
        }

        int votedCount() {
            return this.votedCount;
        }

        Event.BallotValue sentValue() {
            return this.sentValue;
        }

        void notifyChosen() {
            if (logger.isDebugEnabled()) {
                logger.debug("S{} Notify instance {} chosen", context.squadId(), Proposer.this.instanceId);
            }

            //Then notify other peers
            Event notify = new Event.ChosenNotify(Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, this.proposal);
            Proposer.this.config.getCommunicator().selfFirstBroadcast(notify);
        }
    }


    private static class PrepareResult {
        public static PrepareResult of(int proposal, Event.BallotValue content, long ballotId) {
            return new PrepareResult(proposal, content, ballotId);
        }

        final int ballot;
        final Event.BallotValue content;
        final long ballotId;

        private PrepareResult(int ballot, Event.BallotValue content, long ballotId) {
            this.ballot = ballot;
            this.content = content;
            this.ballotId = ballotId;
        }
    }

    private static class ProposalNumHolder {
        private int serverId;
        private int rangeLength;

        public ProposalNumHolder(int serverId, int rangeLength) {
            checkArgument(serverId > 0 && serverId < rangeLength, "server id %d beyond range");
            this.serverId = serverId;
            this.rangeLength = rangeLength;
        }

        public int getProposal0() {
            return this.serverId;
        }

        public int proposalGreatThan(int proposal) {
            final int r = this.rangeLength;
            return ((proposal / r) + 1) * r + this.serverId;
        }
    }

    static class BallotIdHolder {
        private long highBits;
        private int serialNum;

        public BallotIdHolder(int highBits) {
            this.highBits = ((long) highBits) << 32;
            this.serialNum = 0;
        }

        public long nextId() {
            long r = this.highBits | (this.serialNum & 0xffffffffL);
            this.serialNum++;
            return r;
        }
    }
}
