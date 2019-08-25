package org.jaxos.algo;

import org.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class Proposal {
    private static Logger logger = LoggerFactory.getLogger(Proposal.class);

    private JaxosConfig config;

    public Proposal(JaxosConfig config) {
        this.config = config;
    }

    public void propose(long instanceId, byte[] value){

    }

    public void onPrepareReply(Event.PrepareResponse response){
        logger.info("got PREPARE reply {}", response);
    }

    public void onAcceptReply(Event.AcceptResponse event) {
        logger.info("got ACCEPT reply {}", event);
    }
}
