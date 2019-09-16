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
public class Squad implements EventDispatcher,Proponent {
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private Acceptor acceptor;
    private Proposer proposer;
    private SquadContext context;
    private JaxosSettings config;
    private int squadId = 1;

    public Squad(int id, JaxosSettings config, Supplier<Communicator> communicator, AcceptorLogger acceptorLogger, StateMachine machine) {
        this.config = config;
        this.context = new SquadContext(id, this.config);
        Learner learner = new StateMachineRunner(machine);
        this.proposer = new Proposer(this.config, this.context, learner, communicator);
        this.acceptor = new Acceptor(this.config, this.squadId, learner, acceptorLogger);
    }

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    @Override
    public ProposeResult propose(long instanceId, ByteString v) throws InterruptedException {
        SquadContext.RequestRecord lastRequestRecord = this.context.getLastRequestRecord();
        //logger.info("last request is {}, current is {}", lastRequestInfo, new Date());

        if (this.context.isOtherLeaderActive() && !this.config.ignoreLeader()) {
            return ProposeResult.otherLeader(lastRequestRecord.serverId());
        }
        else {
            return proposer.propose(instanceId, v);
        }
    }

    @Override
    public Event process(Event event) {
        switch (event.code()) {
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest) event);
            }
            case PREPARE_RESPONSE: {
                proposer.processPrepareResponse((Event.PrepareResponse) event);
                return null;
            }
            case ACCEPT: {
                return acceptor.accept((Event.AcceptRequest) event);
            }
            case ACCEPT_RESPONSE: {
                proposer.onAcceptReply((Event.AcceptResponse) event);
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
}
