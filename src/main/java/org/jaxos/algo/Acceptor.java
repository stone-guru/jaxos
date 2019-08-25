package org.jaxos.algo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Acceptor {
    Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private int maxBallot;
    private int acceptedBallot = 2;
    private byte[] acceptedValue = new byte[]{1, 2, 3, 4};

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        logger.info("do prepare {} ", request);

        if(request.ballot() > this.maxBallot){
            this.maxBallot = request.ballot();
        }
        return new Event.PrepareResponse(1, 1000, this.maxBallot == request.ballot(),
                this.maxBallot, this.acceptedBallot, this.acceptedValue);
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request){
        return null;
    }
}
