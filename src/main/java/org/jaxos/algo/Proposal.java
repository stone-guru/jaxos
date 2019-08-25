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

    public void onPrepareResponse(Event.PrepareResponse response){
        logger.info("got response {}", response);
    }
}
