package org.jaxos.algo;

import org.jaxos.JaxosConfig;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class LocalEventCenter implements EventEntryPoint {
    private Acceptor acceptor;
    private Proposal proposal;

    public LocalEventCenter(Proposal proposal, Acceptor acceptor) {
        this.proposal = proposal;
        this.acceptor = acceptor;
    }

    @Override
    public Event process(Event event) {
        switch (event.code()){
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest)event);
            }
            case PREPARE_RESPONSE: {
                proposal.onPrepareReply((Event.PrepareResponse)event);
                return null;
            }
            case ACCEPT: {
                return acceptor.accept((Event.AcceptRequest)event);
            }
            case ACCEPT_RESPONSE:{
                proposal.onAcceptReply((Event.AcceptResponse)event);
                return null;
            }
        }
        return null;
    }

}
