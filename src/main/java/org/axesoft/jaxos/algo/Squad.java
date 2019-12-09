package org.axesoft.jaxos.algo;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Squad is a composition of Proposer and Acceptor, and works as a individual paxos server.
 *
 * @sine 2019/8/25.
 */
public class Squad implements EventDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private Acceptor acceptor;
    private Proposer proposer;
    private SquadContext context;
    private SquadMetrics metrics;
    private JaxosSettings settings;
    private StateMachineRunner stateMachineRunner;
    private Components components;
    private volatile boolean learning;

    public Squad(int squadId, JaxosSettings settings, Components components, StateMachine machine) {
        this.settings = settings;
        this.components = components;
        this.context = new SquadContext(squadId, this.settings);
        this.metrics = new SquadMetrics();
        this.stateMachineRunner = new StateMachineRunner(squadId, machine);
        this.proposer = new Proposer(this.settings, components, this.context, (Learner) stateMachineRunner);
        this.acceptor = new Acceptor(this.settings, components, this.context, (Learner) stateMachineRunner);

        this.learning = false;
    }

    /**
     * The id of this squad
     */
    public int id() {
        return this.context.squadId();
    }

    /**
     * The context of this squad
     *
     * @return not null
     */
    public SquadContext context() {
        return this.context;
    }

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    public ListenableFuture<Void> propose(long instanceId, Event.BallotValue v, boolean ignoreLeader, SettableFuture<Void> resultFuture) {
        attachMetricsListener(resultFuture);

        SquadContext.SuccessRequestRecord lastSuccessRequestRecord = this.context.lastSuccessAccept();

        if (this.context.isOtherLeaderActive() && !this.settings.leaderless() && !ignoreLeader) {
            if (logger.isDebugEnabled()) {
                logger.debug("S{} I{} redirect to {}", context.squadId(), instanceId, lastSuccessRequestRecord.serverId());
            }
            resultFuture.setException(new RedirectException(lastSuccessRequestRecord.serverId()));
        }
        else {
            proposer.propose(instanceId, v, ignoreLeader, resultFuture);
        }

        return resultFuture;
    }

    @Override
    public Event processEvent(Event request) {
        if (request instanceof Event.BallotEvent) {
            Event.BallotEvent ballotRequest = (Event.BallotEvent) request;
            Event.BallotEvent result = processBallotEvent(ballotRequest);
            long last = this.lastChosenInstanceId();
            if (last < ballotRequest.chosenInstanceId() && !learning) {
                startLearn(ballotRequest.senderId(), last, ballotRequest.chosenInstanceId());
            }
            return result;
        }
        else if (request instanceof Event.InstanceEvent) {
            return processLearnerEvent(request);
        }
        else {
            throw new UnsupportedOperationException("Unknown event type of " + request.code());
        }
    }

    private void attachMetricsListener(ListenableFuture<Void> future) {
        final long startNano = System.nanoTime();

        Futures.addCallback(future, new FutureCallback<>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                record(SquadMetrics.ProposalResult.SUCCESS);
            }

            @Override
            public void onFailure(Throwable t) {
                if (t instanceof ProposalConflictException) {
                    record(SquadMetrics.ProposalResult.CONFLICT);
                }
                else {
                    record(SquadMetrics.ProposalResult.OTHER);
                }
            }

            private void record(SquadMetrics.ProposalResult result) {
                Squad.this.metrics.recordPropose(System.nanoTime() - startNano, result);
            }

        }, MoreExecutors.directExecutor());
    }

    public long lastChosenInstanceId() {
        return this.acceptor.lastChosenInstanceId();
    }

    private Event processLearnerEvent(Event event) {
        switch (event.code()) {
            case CHOSEN_QUERY_RESPONSE: {
                long otherLast = ((Event.ChosenQueryResponse) event).chosenInstanceIdOf(context.squadId());
                long last = this.lastChosenInstanceId();
                if (last < otherLast - 1 && !learning) {
                    startLearn(event.senderId(), last, otherLast);
                }
                return null;
            }
            case LEARN_REQUEST: {
                return this.onLearnRequest((Event.Learn) event);
            }
            case LEARN_RESPONSE: {
                this.onLearnResponse((Event.LearnResponse) event);
                this.learning = false;
                return null;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }

    private Event.BallotEvent processBallotEvent(Event.BallotEvent event) {
        switch (event.code()) {
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest) event);
            }
            case PREPARE_RESPONSE: {
                proposer.onPrepareReply((Event.PrepareResponse) event);
                return null;
            }
            case PREPARE_TIMEOUT: {
                proposer.onPrepareTimeout((Event.PrepareTimeout) event);
                return null;
            }
            case ACCEPT: {
                long nano = System.nanoTime();
                Event.BallotEvent e = acceptor.accept((Event.AcceptRequest) event);
                this.metrics.recordAccept(System.nanoTime() - nano);
                return e;
            }
            case ACCEPT_RESPONSE: {
                proposer.onAcceptReply((Event.AcceptResponse) event);
                return null;
            }
            case ACCEPT_TIMEOUT: {
                proposer.onAcceptTimeout((Event.AcceptTimeout) event);
                return null;
            }
            case ACCEPTED_NOTIFY: {
                acceptor.onChosenNotify(((Event.ChosenNotify) event));
                return null;
            }
            case PROPOSAL_TIMEOUT: {
                proposer.onProposalTimeout((Event.ProposalTimeout) event);
                return null;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }

    private void startLearn(int senderId, long myLast, long otherLast) {
        this.components.getWorkerPool().queueInstanceTask(() -> {
            Event.Learn learn = new Event.Learn(settings.serverId(), context.squadId(), myLast + 1, otherLast);
            this.components.getCommunicator().send(learn, senderId);
            logger.info("Sent learn request {}", learn);
        });
    }


    private Event onLearnRequest(Event.Learn request) {
        ImmutableList.Builder<Instance> builder = ImmutableList.builder();

        boolean instanceMissing = false;
        for (long id = request.lowInstanceId(); id <= request.highInstanceId(); id++) {
            Instance p = this.components.getLogger().loadPromise(context.squadId(), id);
            if (p.isEmpty()) {
                instanceMissing = true;

                if(logger.isDebugEnabled()) {
                    logger.debug("Making learn response, Server {} lack instance {} of squad {}, give checkpoint", settings.serverId(), id, context.squadId());
                }
                break;
            }

            builder.add(p);
        }

        if (instanceMissing) {
            CheckPoint checkPoint = this.stateMachineRunner.makeCheckPoint(context.squadId());
            return new Event.LearnResponse(settings.serverId(), context.squadId(), Collections.emptyList(), checkPoint);
        }
        else {
            Event.LearnResponse resp = new Event.LearnResponse(settings.serverId(), context.squadId(), builder.build(), CheckPoint.EMPTY);
            logger.info("squad {} prepared learn response from {} to {}", context.squadId(),
                    resp.lowInstanceId(), resp.highInstanceId());
            return resp;
        }
    }

    private void onLearnResponse(Event.LearnResponse response) {
        logger.info("squad {} learn CheckPoint {} with instances from {} to {}",
                context.squadId(), response.checkPoint().instanceId(),
                response.lowInstanceId(), response.highInstanceId());
        this.stateMachineRunner.restoreFromCheckPoint(response.checkPoint(), response.instances());
    }

    public void computeAndPrintMetrics(long current) {
        double seconds = (current - this.metrics.lastTime()) / 1000.0;
        double successRate = this.metrics.successRate();
        double conflictRate = this.metrics.conflictRate();
        double otherRate = metrics.otherRate();
        long proposalDelta = this.metrics.proposeDelta();
        long acceptDelta = this.metrics.acceptDelta();

        Pair<Double, Double> elapsed = this.metrics.compute(current);

        String msg = String.format("S %d, L=%d, PT=%d, PE=%.3f, S=%.2f, C=%.2f, O=%.2f, AE=%.3f, AT=%d (%.0f s), TT=%d, SR=%.3f, LI=%d",
                this.context.squadId(), context.lastSuccessAccept().serverId(),
                proposalDelta, elapsed.getLeft(),
                successRate, conflictRate, otherRate, elapsed.getRight(), acceptDelta, seconds,
                this.metrics.proposeTimes(), this.metrics.totalSuccessRate(), this.acceptor.lastChosenInstanceId()
        );
        logger.info(msg);
    }

    public void saveCheckPoint() {
        CheckPoint checkPoint = this.stateMachineRunner.makeCheckPoint(context.squadId());
        this.components.getLogger().saveCheckPoint(checkPoint);

        logger.info("{} saved", checkPoint);
    }

    public void restoreFromDB() {
        CheckPoint checkPoint = this.components.getLogger().loadLastCheckPoint(context.squadId());
        long last = this.components.getLogger().loadLastPromise(context.squadId()).instanceId();

        List<Instance> ix = new ArrayList<>();
        for (long i = checkPoint.instanceId() + 1; i <= last; i++) {
            Instance instance = this.components.getLogger().loadPromise(context.squadId(), i);
            if (instance.isEmpty()) {
                String msg = String.format("Instance %d.%d not found in DB", context.squadId(), i);
                throw new IllegalStateException(msg);
            }
            ix.add(instance);
        }

        this.stateMachineRunner.restoreFromCheckPoint(checkPoint, ix);

        logger.info("Squad {} restored to instance {}", context.squadId(), last);
    }
}
