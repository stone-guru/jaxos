package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Acceptor {
    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private volatile long instanceId;
    private volatile int maxBallot;
    private volatile int acceptedBallot;
    private volatile ByteString acceptedValue;
    private InstanceContext instanceContext;
    private JaxosConfig config;
    private AcceptorLogger acceptorLogger;

    public Acceptor(JaxosConfig config, InstanceContext instanceContext, AcceptorLogger acceptorLogger) {
        this.config = config;
        this.maxBallot = 0;
        this.acceptedValue = ByteString.EMPTY;
        this.instanceContext = instanceContext;
        this.acceptorLogger = acceptorLogger;

        AcceptorLogger.Promise promise = this.acceptorLogger.loadLastPromise(instanceContext.squadId());
        if(promise != null){
            logger.info("Acceptor restore last instance {}", promise.instanceId);
            instanceContext.learnValue(promise.instanceId, promise.value);
        }
    }

    public synchronized Event.PrepareResponse prepare(Event.PrepareRequest request) {
        logger.debug("do prepare {} ", request);
        long last = this.instanceContext.lastInstanceId();

        if (request.instanceId() < last) {
            logger.warn("historical instance id in prepare(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            ByteString v = instanceContext.valueOf(request.instanceId());
            return new Event.PrepareResponse(config.serverId(), request.instanceId(), false, 0, Integer.MAX_VALUE, v);
        }
        else if (request.instanceId() >= last + 1) {
            //It's ok to start a new round
            if (this.instanceId == 0) {
                if (request.instanceId() == last + 1) {
                    logger.debug("PREPARE start a new round with instance id {}", request.instanceId());
                }
                else {
                    logger.warn("PREPARE start a new round with instance id {}, while my last instance id is {}", request.instanceId(), last);
                }
                this.instanceId = request.instanceId();
            }
            else if (this.instanceId != request.instanceId()) {
                logger.error("Something is wrong when prepare last = {}, round instance id = {}, request instance id = {}",
                        last, this.instanceId, request.instanceId());
                return null;
            }
        }

        boolean success = false;
        int b0 = this.maxBallot;
        if(request.ballot() > this.maxBallot) {
            this.maxBallot = request.ballot();
            success = true;
            acceptorLogger.saveLastPromise(instanceContext.squadId(), this.instanceId, request.ballot(), this.acceptedValue);
        }

        return new Event.PrepareResponse(config.serverId(), this.instanceId, success, b0, acceptedBallot, acceptedValue);
    }

    public synchronized Event.AcceptResponse accept(Event.AcceptRequest request) {
        //no prepare before
        if (this.instanceId <= 0) {
            long last = this.instanceContext.lastInstanceId();

            if (request.instanceId() > last + 1) {
                logger.warn("ACCEPT future instance id in accept(instance id = {}), while my last instance id is {} ",
                        request.instanceId(), last);
                this.instanceId = request.instanceId();
            }
            else if (request.instanceId() <= last) {
                logger.error("ACCEPT historical instance id in accept(instance id = {}), while my instance id is {} ",
                        request.instanceId(), last);
                return new Event.AcceptResponse(config.serverId(), request.instanceId(), Integer.MAX_VALUE, false);
            }
            else if (request.instanceId() == last + 1) {
                //It's ok to start a new round
                logger.debug("ACCEPT start a new round with instance id {}", request.instanceId());
                this.instanceId = request.instanceId();
            }
            else {
                logger.error("ACCEPT uncovered case");
                return null;
            }
        }
        //prepared but instance id in request is not same, abandon the request
        else if (this.instanceId != request.instanceId()) {
            logger.error("unmatched instance id in accept(instance id = {}), while my instance id is {} ",
                    request.instanceId(), this.instanceId);
            return null;
        }

        if(request.ballot() < this.maxBallot){
            logger.info("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
            return new Event.AcceptResponse(config.serverId(), this.instanceId, this.maxBallot, false);
        }

        this.acceptedBallot = this.maxBallot = request.ballot();
        this.acceptedValue = request.value();
        logger.debug("Accept new value sender = {}, ballot = {}, value = {}", request.senderId(), acceptedBallot, acceptedValue.toStringUtf8());

        this.instanceContext.recordLastRequest(request.senderId(), System.currentTimeMillis());

        acceptorLogger.saveLastPromise(instanceContext.squadId(), this.instanceId, this.maxBallot, this.acceptedValue);

        return new Event.AcceptResponse(config.serverId(), this.instanceId, this.maxBallot, true);
    }

    public synchronized void chose(Event.ChosenNotify notify) {
        logger.debug("receive chose notify {}", notify);
        if (notify.instanceId() != this.instanceId) {
            logger.debug("unmatched instance id in chose notify({}), while my instance id is {} ", notify.instanceId(), this.instanceId);
            return;
        }

        instanceContext.learnValue(this.instanceId, this.acceptedValue);

        this.instanceId = 0;
        this.acceptedValue = ByteString.EMPTY;
        this.maxBallot = 0;
    }
}
