package org.axesoft.jaxos.algo;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.base.LongRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class Squad implements EventDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private Acceptor acceptor;
    private Proposer proposer;
    private SquadContext context;
    private SquadMetrics metrics;
    private JaxosSettings config;
    private StateMachineRunner stateMachineRunner;
    private AcceptorLogger acceptorLogger;
    private Supplier<EventWorkerPool> workerPoolSupplier;
    private Supplier<Communicator> communicator;
    private boolean studying;

    public Squad(int squadId, JaxosSettings config, Supplier<Communicator> communicator, AcceptorLogger acceptorLogger, StateMachine machine, Supplier<EventWorkerPool> workerPoolSupplier) {
        this.config = config;
        this.workerPoolSupplier = workerPoolSupplier;
        this.communicator = communicator;
        this.context = new SquadContext(squadId, this.config);
        this.metrics = new SquadMetrics();
        this.stateMachineRunner = new StateMachineRunner(machine);
        this.proposer = new Proposer(this.config, this.context, (Learner) stateMachineRunner, communicator, () -> this.workerPoolSupplier.get());
        this.acceptor = new Acceptor(this.config, this.context, (Learner) stateMachineRunner, acceptorLogger);
        this.acceptorLogger = acceptorLogger;

        this.studying = false;
    }

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    public ProposeResult propose(long instanceId, ByteString v) throws InterruptedException {
        final long startNano = System.nanoTime();

        SquadContext.SuccessRequestRecord lastSuccessRequestRecord = this.context.lastSuccessPrepare();
        //logger.info("last request is {}, current is {}", lastRequestInfo, new Date());

        ProposeResult result;
        if (this.context.isOtherLeaderActive() && !this.config.ignoreLeader()) {
            result = ProposeResult.otherLeader(lastSuccessRequestRecord.serverId());
        }
        else {
            result = proposer.propose(instanceId, v);
        }

        this.metrics.recordPropose(System.nanoTime() - startNano, result);

        return result;
    }

    @Override
    public Event process(Event request) {
        if (logger.isTraceEnabled()) {
            logger.trace("Process event {}", request);
        }

        Event result = dispatch(request);

        if (result != null && !studying) {
            final LongRange r = tryCalcuLag(request, result);
            if (r != null) {
                logger.info("Found lag {} ", r);
                this.studying = true;
                this.workerPoolSupplier.get().queueTask(context.squadId(),
                        () -> {
                            Event.Learn learn = new Event.Learn(config.serverId(), context.squadId(), r.low(), r.high());
                            communicator.get().send(learn, request.senderId());
                            logger.info("Sent learn request {}", learn);
                        }
                );
            }
        }

        return result;
    }

    private Event dispatch(Event event) {
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
                return acceptor.accept((Event.AcceptRequest) event);
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
                acceptor.onChoseNotify(((Event.ChosenNotify) event));
                return null;
            }
            case LEARN: {
                return this.teach((Event.Learn) event);
            }
            case LEARN_RESPONSE: {
                this.learn((Event.LearnResponse) event);
                this.studying = false;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }

    private LongRange tryCalcuLag(Event request, Event response) {
        long localChosenInstanceId = 0;
        if (response.code() == Event.Code.PREPARE_RESPONSE) {
            localChosenInstanceId = ((Event.PrepareResponse) response).chosenInstanceId();
        }
        else if (response.code() == Event.Code.ACCEPT_RESPONSE) {
            localChosenInstanceId = ((Event.AcceptResponse) response).chosenInstanceId();
        }
        else {
            return null;
        }
        long requestInstanceId = ((Event.BallotEvent) request).instanceId();
        if (requestInstanceId == localChosenInstanceId + 1) {
            return null;
        }

        return new LongRange(localChosenInstanceId + 1, requestInstanceId);
    }

    private Event teach(Event.Learn request) {
        ImmutableList.Builder<Pair<Long, ByteString>> builder = ImmutableList.builder();

        for (long id = request.lowInstanceId(); id <= request.highInstanceId(); id++) {
            AcceptorLogger.Promise p = this.acceptorLogger.loadPromise(context.squadId(), id);
            if (p == null) {
                logger.warn("{} lack instance {} of squad {}", config.serverId(), id, context.squadId());
                break;
            }
            builder.add(Pair.of(p.instanceId, p.value));
        }

        logger.info("squad {} prepared learn response from {} to {}", context.squadId(),
                request.lowInstanceId(), request.highInstanceId());

        return new Event.LearnResponse(config.serverId(), context.squadId(), builder.build());
    }

    private void learn(Event.LearnResponse response) {
        logger.info("squad {} learn instances from {} to {}", context.squadId(),
                response.lowInstanceId(), response.highInstanceId());

        for (Pair<Long, ByteString> p : response.instances()) {
            this.stateMachineRunner.learnValue(context.squadId(), p.getLeft(), Integer.MAX_VALUE, p.getRight());
        }
    }

    public void computeAndPrintMetrics(long current) {
        double seconds = (current - this.metrics.lastTime()) / 1000.0;
        double successRate = this.metrics.successRate();
        double conflictRate = this.metrics.conflictRate();
        double otherRate = metrics.otherRate();
        long delta = this.metrics.delta();
        double elapsed = this.metrics.compute(current);

        String msg = String.format("ID=%d, T=%d, E=%.3f, S=%.2f, C=%.2f, O=%.2f in %.0f sec, TT=%d, SR=%.3f, LI=%d",
                this.context.squadId(), delta, elapsed,
                successRate, conflictRate, otherRate, seconds,
                this.metrics.times(), this.metrics.totalSuccessRate(), this.acceptor.lastChosenInstanceId());
        logger.info(msg);
    }

    public void saveCheckPoint() {
        CheckPoint checkPoint = this.stateMachineRunner.machine().makeCheckPoint(context.squadId());
        this.acceptorLogger.saveCheckPoint(checkPoint);

        logger.info("{} saved", checkPoint);
    }

    public void restoreFromDB() {
        CheckPoint checkPoint = this.acceptorLogger.loadLastCheckPoint(context.squadId());
        long lastInstanceId = 0;
        if (checkPoint != null) {
            lastInstanceId = checkPoint.instanceId();
            this.stateMachineRunner.machine().restoreFromCheckPoint(checkPoint);
            this.stateMachineRunner.learnLastChosen(context.squadId(), lastInstanceId, Integer.MAX_VALUE);
            logger.info("Restore to last {}", checkPoint);
        }

        AcceptorLogger.Promise p0 = this.acceptorLogger.loadLastPromise(context.squadId());
        if (p0 == null) {
            return;
        }

        if (p0.instanceId == checkPoint.instanceId()) {
            this.stateMachineRunner.learnLastChosen(p0.squadId, p0.instanceId, p0.proposal);
        }
        else {
            for (long i = lastInstanceId + 1; i <= p0.instanceId; i++) {
                AcceptorLogger.Promise p = this.acceptorLogger.loadPromise(context.squadId(), i);
                if (p == null) {
                    logger.error("Promise(" + i + ") not found in DB");
                    break;
                }
                lastInstanceId = p.instanceId;
                this.stateMachineRunner.learnValue(p.squadId, i, p.proposal, p.value);
            }
        }
        logger.info("Squad {} restored to instance {}", context.squadId(), lastInstanceId);
    }
}
