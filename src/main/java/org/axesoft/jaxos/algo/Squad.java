package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class Squad implements EventDispatcher{
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private Acceptor acceptor;
    private Proposer proposer;
    private SquadContext context;
    private SquadMetrics metrics;
    private JaxosSettings config;


    public Squad(int squadId, JaxosSettings config, Supplier<Communicator> communicator, AcceptorLogger acceptorLogger, StateMachine machine, Supplier<EventTimer> timerSupplier) {
        this.config = config;
        this.context = new SquadContext(squadId, this.config);
        this.metrics = new SquadMetrics();
        Learner learner = new StateMachineRunner(machine);
        this.proposer = new Proposer(this.config, this.context, learner, communicator, timerSupplier);
        this.acceptor = new Acceptor(this.config, this.context, learner, acceptorLogger);

    }

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    public ProposeResult propose(long instanceId, ByteString v) throws InterruptedException {
        final long startNano = System.nanoTime();

        SquadContext.SuccessRequestRecord lastSuccessRequestRecord = this.context.lastSuccessPrepare();
        //logger.info("last request is {}, current is {}", lastRequestInfo, new Date());

        ProposeResult result ;
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
    public Event process(Event event) {
        switch (event.code()) {
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest) event);
            }
            case PREPARE_RESPONSE: {
                proposer.onPrepareReply((Event.PrepareResponse) event);
                return null;
            }
            case PREPARE_TIMEOUT: {
                proposer.onPrepareTimeout((Event.PrepareTimeout)event);
                return null;
            }
            case ACCEPT: {
                return acceptor.accept((Event.AcceptRequest) event);
            }
            case ACCEPT_RESPONSE: {
                proposer.onAcceptReply((Event.AcceptResponse) event);
                return null;
            }
            case ACCEPT_TIMEOUT:{
                proposer.onAcceptTimeout((Event.AcceptTimeout)event);
                return null;
            }
            case ACCEPTED_NOTIFY: {
                acceptor.onChoseNotify(((Event.ChosenNotify) event));
                return null;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }


    public void computeAndPrintMetrics(long current) {
        double seconds = (current - this.metrics.lastTime()) / 1000.0;
        double successRate = this.metrics.successRate();
        double conflictRate = this.metrics.conflictRate();
        double otherRate = metrics.otherRate();
        long delta = this.metrics.delta();
        double elapsed = this.metrics.compute(current);

        String msg = String.format("ID=%d, T=%d, E=%.3f, S=%.2f, C=%.2f, O=%.2f in %.0f sec, Total %d s and success rate %.3f",
                this.context.squadId(), delta, elapsed,
                successRate, conflictRate, otherRate, seconds,
                this.metrics.times(), this.metrics.totalSuccessRate());
        logger.info(msg);
    }
}
