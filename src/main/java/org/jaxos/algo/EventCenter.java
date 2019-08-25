package org.jaxos.algo;

import org.jaxos.JaxosConfig;
import org.jaxos.network.EventEntryPoint;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class EventCenter implements EventEntryPoint {
    private JaxosConfig config;
    private Acceptor acceptor;
    private Proposal proposal;

    public EventCenter(JaxosConfig config) {
        this.config = config;
        this.proposal = new Proposal(this.config);
        this.acceptor = new Acceptor();
    }


    @Override
    public Event process(Event event) {
        switch (event.code()){
            case PREPARE: {
                return acceptor.prepare((Event.PrepareRequest)event);
            }
            case PREPARE_RESPONSE: {
                proposal.onPrepareResponse((Event.PrepareResponse)event);
            }
            case ACCEPT: {
                return acceptor.accept((Event.AcceptRequest)event);
            }
            case ACCEPT_RESPONSE:{
                proposal.onAcceptReply((Event.AcceptResponse)event);
            }
        }
        return null;
    }

}
