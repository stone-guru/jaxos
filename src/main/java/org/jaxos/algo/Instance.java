package org.jaxos.algo;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.jaxos.JaxosConfig;

import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class Instance implements EventEntryPoint {
    private Acceptor acceptor;
    private Proposal proposal;
    private InstanceContext context;

    public Instance(JaxosConfig config, Supplier<Communicator> communicator) {
        this.context = new InstanceContext();
        this.proposal = new Proposal(config, context, communicator);
        this.acceptor = new Acceptor(config, context);
    }

    /**
     * @param v value to be proposed
     * @throws CommunicatorException
     */
    public void propose(ByteString v) throws InterruptedException{
        this.proposal.propose(v);
    }

    public long lastChosenInstance(){
        return context.lastInstanceId();
    }

    @Override
    public Event process(Event event) {
        switch (event.code()) {
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest) event);
            }
            case PREPARE_RESPONSE: {
                proposal.onPrepareReply((Event.PrepareResponse) event);
                return null;
            }
            case ACCEPT: {
                return acceptor.accept((Event.AcceptRequest) event);
            }
            case ACCEPT_RESPONSE: {
                proposal.onAcceptReply((Event.AcceptResponse) event);
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
