package org.jaxos.algo;

import org.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Acceptor {
    Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private int maxBallot;
    private int acceptedBallot = 0 ;
    private byte[] acceptedValue = new byte[0];
    private JaxosConfig config;

    public Acceptor(JaxosConfig config) {
        this.config = config;
    }

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        logger.info("do prepare {} ", request);

        if (request.ballot() > this.maxBallot) {
            this.maxBallot = request.ballot();
        }
        return new Event.PrepareResponse(config.serverId(), 1000, this.maxBallot == request.ballot(),
                this.maxBallot, this.acceptedBallot, this.acceptedValue);
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request) {
        boolean accepted = false;
        if (request.ballot() >= this.maxBallot) {
            this.maxBallot = request.ballot();
            this.acceptedBallot = this.maxBallot;
            this.acceptedValue = Arrays.copyOf(request.value(), request.value().length);
            accepted = true;

            String v = new String(this.acceptedValue);
            logger.info("Accept new value ballot = {}, value = {}", this.acceptedBallot, v);
        }
        else {
            logger.info("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
        }

        return new Event.AcceptResponse(config.serverId(), 1000, this.maxBallot, accepted);
    }
}
