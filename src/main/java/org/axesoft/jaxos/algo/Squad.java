package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class Squad implements EventEntryPoint {
    private static final Logger logger = LoggerFactory.getLogger(Squad.class);

    private Acceptor acceptor;
    private Proposer proposer;
    private InstanceContext context;
    private JaxosConfig config;

    public Squad(int id, JaxosConfig config, Supplier<Communicator> communicator, AcceptorLogger acceptorLogger) {
        this.config = config;
        this.context = new InstanceContext(id, this.config);
        this.proposer = new Proposer(this.config, context, communicator);
        this.acceptor = new Acceptor(this.config, context, acceptorLogger);
    }

    /**
     * @param v value to be proposed
     * @throws InterruptedException
     */
    public ProposeResult propose(ByteString v) throws InterruptedException {
        InstanceContext.RequestRecord lastRequestRecord = this.context.getLastRequestRecord();
        //logger.info("last request is {}, current is {}", lastRequestInfo, new Date());

        if (this.context.isOtherLeaderActive() && !this.config.ignoreLeader()) {
            return ProposeResult.notLeader(config.getPeer(lastRequestRecord.serverId()));
        }
        else {
            return proposer.propose(v);
        }
    }

    public long lastChosenInstance() {
        return context.lastInstanceId();
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
            case ACCEPT: {
                return acceptor.accept((Event.AcceptRequest) event);
            }
            case ACCEPT_RESPONSE: {
                proposer.onAcceptReply((Event.AcceptResponse) event);
                return null;
            }
            case ACCEPTED_NOTIFY: {
                acceptor.chose(((Event.ChosenNotify) event));
                return null;
            }
            default: {
                throw new UnsupportedOperationException(event.code().toString());
            }
        }
    }
}
