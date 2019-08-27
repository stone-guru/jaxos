package org.jaxos.algo;

import org.apache.commons.lang3.tuple.Pair;
import org.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class Proposal {
    public static final int NONE = 0;
    public static final int PREPARING = 1;
    public static final int ACCEPTING = 2;
    public static final int ACCEPTED = 3;

    private static Logger logger = LoggerFactory.getLogger(Proposal.class);

    private Map<Integer, Event.PrepareResponse> acceptResponses = new ConcurrentHashMap<>();

    private JaxosConfig config;
    private Supplier<Communicator> communicator;
    private AtomicInteger state ;
    private AtomicInteger ballot;

    private int totalMaxBallot = 0;
    private int acceptedMaxBallot = 0;
    private byte[] acceptedValue;

    private byte[] proposeValue;

    public Proposal(JaxosConfig config, Supplier<Communicator> communicator) {
        this.config = config;
        this.communicator = communicator;
        this.state = new AtomicInteger(NONE);
        this.ballot =  new AtomicInteger(this.config.serverId());
    }

    public void propose(byte[] value){
        this.proposeValue = value;
        state.set(PREPARING);
        Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), 1000, ballot.get());
        communicator.get().broadcast(req);
    }

    public void onPrepareReply(Event.PrepareResponse response){
        if(state.get() != PREPARING){
            logger.warn("receive PREPARE reply not preparing {}", response);
            return;
        }

        if(acceptResponses.putIfAbsent(response.senderId(), response) != null) {
                logger.warn("Duplicated PREPARE response {}", response);
            return;
        }

        if(response.maxBallot() > this.totalMaxBallot){
            this.totalMaxBallot = response.maxBallot();
        }

        if(response.acceptedBallot() > this.acceptedMaxBallot){
            this.acceptedMaxBallot = response.acceptedBallot();
            this.acceptedValue = response.acceptedValue();
            logger.info("There are another accepted value {}", new String(response.acceptedValue()));
        }

        if(acceptResponses.size() == config.peerMap().size() + 1) {
            logger.info("End prepare");
        }
        logger.info("got PREPARE reply {}", response);
    }

    public void onAcceptReply(Event.AcceptResponse event) {
        logger.info("got ACCEPT reply {}", event);
    }
}
