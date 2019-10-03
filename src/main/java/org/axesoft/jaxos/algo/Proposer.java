package org.axesoft.jaxos.algo;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.netty.util.Timeout;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.base.IntBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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

    private Timeout proposalTimeout;
    private PrepareActor prepareActor;
    private Timeout prepareTimeout;
    private AcceptActor acceptActor;
    private Timeout acceptTimeout;

    private ByteString proposeValue;
    private long instanceId = 0;
    private int messageMark = 0;
    private int round = 0;
    private Stage stage;
    private AtomicReference<SettableFuture<Void>> resultFutureRef;

    public Proposer(JaxosSettings settings, Configuration config, SquadContext context, Learner learner) {
        this.settings = settings;
        this.config = config;
        this.context = context;
        this.learner = learner;
        this.proposalNumHolder = new ProposalNumHolder(this.settings.serverId(), JaxosSettings.SERVER_ID_RANGE);
        this.stage = Stage.NONE;
        this.prepareActor = new PrepareActor();
        this.acceptActor = new AcceptActor();
        this.resultFutureRef = new AtomicReference<>(null);
    }

    public ListenableFuture<Void> propose(long instanceId, ByteString value, SettableFuture<Void> resultFuture) {
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

        if (settings.ignoreLeader()) {
            startPrepare(proposalNumHolder.getProposal0());
        }
        else if (context.isOtherLeaderActive()) {
            return endAs(new RedirectException(context.lastSuccessPrepare().serverId()));
        }
        else if (context.isLeader()) {
            startAccept(this.proposeValue, context.lastSuccessPrepare().proposal(), settings.serverId());
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
        if (instanceId != chosen.instanceId + 1) {
            String msg = String.format("when prepare instance %d while last chosen is %d.%d",
                    context.squadId(), instanceId, chosen.instanceId);
            endAs(new ProposalConflictException(msg));
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
        if (endWithMajorityCheck(this.prepareActor.votedCount(), "PREPARE " + instanceId)) {
            return;
        }

        PrepareResult v = this.prepareActor.getResult();
        if (this.prepareActor.isAccepted()) {
            startAccept(v.content, this.prepareActor.myProposal(), v.proposer);
        }
        else {
            if (this.prepareActor.totalMaxProposal == Integer.MAX_VALUE) {
                if (v.proposer == settings.serverId()) {
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
            startPrepare(proposalNumHolder.nextProposal(this.prepareActor.totalMaxProposal()));
        }
    }

    private void startAccept(ByteString value, int proposal, int proposer) {
        Learner.LastChosen chosen = this.learner.lastChosen(this.context.squadId());
        if (instanceId != chosen.instanceId + 1) {
            String msg = String.format("when accept instance %d.%d while last chosen is %d",
                    context.squadId(), instanceId, chosen.instanceId);
            endAs(new ProposalConflictException(msg));
            return;
        }
        this.stage = Stage.ACCEPTING;
        this.acceptActor.startAccept(value, proposal, proposer, chosen.proposal);

        this.acceptTimeout = config.getEventTimer().createTimeout(this.settings.acceptTimeoutMillis(), TimeUnit.MILLISECONDS,
                new Event.AcceptTimeout(settings.serverId(), this.context.squadId(), this.instanceId, this.messageMark));
    }

    void onAcceptReply(Event.AcceptResponse response) {
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

        if (endWithMajorityCheck(this.acceptActor.votedCount(), "ACCEPT")) {
            return;
        }

        if (this.acceptActor.isAccepted()) {
            this.acceptActor.notifyChosen();
            if (this.acceptActor.proposer == settings.serverId()) {
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
            startPrepare(this.proposalNumHolder.nextProposal(acceptActor.maxProposal));
        }
    }

    private void sleepRandom(int i, String when) {
        try {
            long t = (long) (Math.random() * 150);
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
        private int valueProposer = 0;

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
            //this.valueProposer = valueProposer;
            this.chosenProposal = chosenProposal;

            Event.PrepareRequest req = new Event.PrepareRequest(
                    Proposer.this.settings.serverId(), Proposer.this.context.squadId(),
                    Proposer.this.instanceId, Proposer.this.messageMark,
                    this.proposal, this.chosenProposal);

            config.getCommunicator().broadcast(req);
        }

        public void onReply(Event.PrepareResponse response) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{}: On PREPARE reply {}", context.squadId(), response);
            }
            repliedNodes.add(response.senderId());

            if (response.result() == Event.RESULT_STANDBY) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{}: On PrepareReply: Server {} is standby at last chosen instance id is {}",
                            context.squadId(), response.senderId(), response.chosenInstanceId());
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
                this.valueProposer = response.valueProposer();
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

        private long maxOtherChosenInstanceId() {
            return this.maxOtherChosenInstanceId;
        }

        private PrepareResult getResult() {
            if (logger.isTraceEnabled()) {
                logger.trace("S{} Instance {} on all prepared max accepted ballot = {}, total max ballot ={}, my ballot = {}",
                        context.squadId(), instanceId, maxAcceptedProposal, totalMaxProposal, this.proposal);
            }

            ByteString value;
            if (this.maxAcceptedProposal == 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("S{} Result of prepare({}) No other chose value, total max ballot is {}",
                            context.squadId(), Proposer.this.instanceId, this.totalMaxProposal);
                }
                return PrepareResult.of(0, this.value, settings.serverId());
            }
            else {
                if (logger.isTraceEnabled()) {
                    logger.trace("S{}: Result of prepare({}), other accepted value with proposal {}",
                            context.squadId(), Proposer.this.instanceId, maxAcceptedProposal);
                }
                return new PrepareResult(this.totalMaxProposal, this.acceptedValue, this.valueProposer);
            }
        }
    }

    private class AcceptActor {
        private ByteString sentValue;
        private int proposal;
        private int proposer;

        private int maxProposal = 0;
        private boolean someoneReject = true;
        private IntBitSet repliedNodes = new IntBitSet();
        private int acceptedCount = 0;
        private boolean instanceChosen;
        private int votedCount = 0;


        public void startAccept(ByteString value, int proposal, int proposer, int lastChosenProposal) {
            this.proposal = proposal;
            this.sentValue = value;
            this.proposer = proposer;
            this.maxProposal = 0;
            this.someoneReject = false;
            this.repliedNodes.clear();
            this.acceptedCount = 0;
            this.instanceChosen = false;
            this.votedCount = 0;

            if (logger.isDebugEnabled()) {
                logger.debug("S{}: Start accept instance {} with proposal  {} ", context.squadId(), Proposer.this.instanceId, proposal);
            }

            Event.AcceptRequest request = new Event.AcceptRequest(
                    Proposer.this.settings.serverId(), Proposer.this.context.squadId(), Proposer.this.instanceId, Proposer.this.messageMark,
                    proposal, value, this.proposer, lastChosenProposal);
            Proposer.this.config.getCommunicator().broadcast(request);
        }

        public void onReply(Event.AcceptResponse response) {
            this.repliedNodes.add(response.senderId());

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
            return this.repliedNodes.count() == settings.peerCount();
        }

        boolean isInstanceChosen() {
            return this.instanceChosen;
        }

        int votedCount() {
            return this.votedCount;
        }

        ByteString sentValue() {
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
        public static final PrepareResult NONE = new PrepareResult(0, ByteString.EMPTY, 0);

        public static PrepareResult of(int proposal, ByteString content, int proposer) {
            return new PrepareResult(proposal, content, proposer);
        }

        final int ballot;
        final ByteString content;
        final int proposer;

        public PrepareResult(int ballot, ByteString content, int proposer) {
            this.ballot = ballot;
            this.content = content;
            this.proposer = proposer;
        }
    }
}
