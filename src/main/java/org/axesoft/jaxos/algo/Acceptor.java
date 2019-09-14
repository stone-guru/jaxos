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
        if (promise != null) {
            logger.info("Acceptor restore last instance {}", promise.instanceId);
            instanceContext.learnValue(promise.instanceId, promise.proposal, promise.value);
        }
    }

    public synchronized Event.PrepareResponse prepare(Event.PrepareRequest request) {
        logger.trace("On prepare {} ", request);
        this.instanceContext.recordLastRequest(request.senderId(), System.currentTimeMillis());

        long last = this.instanceContext.lastInstanceId();

        if (request.instanceId() <= last) {
            logger.warn("PrepareResponse: instance id in prepare(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            ValueWithProposal v = instanceContext.valueOf(request.instanceId());
            return new Event.PrepareResponse(config.serverId(), request.instanceId(), false, 0, Integer.MAX_VALUE, v.content);
        }
        else if (request.instanceId() > last + 1) {
            logger.error("PrepareResponse: future instance id in prepare(instance id = {}), request instance id = {}", last, request.instanceId());
            return null;
        }
        else { // request.instanceId == last + 1
            boolean success = false;
            int b0 = this.maxBallot;
            if (request.ballot() > this.maxBallot) {
                this.maxBallot = request.ballot();
                success = true;
                acceptorLogger.saveLastPromise(instanceContext.squadId(), request.instanceId(), request.ballot(), this.acceptedValue);
            }

            return new Event.PrepareResponse(config.serverId(), request.instanceId(), success, b0, acceptedBallot, acceptedValue);
        }
    }

    public synchronized Event.AcceptResponse accept(Event.AcceptRequest request) {
        logger.trace("On Accept {}", request);

        this.instanceContext.recordLastRequest(request.senderId(), System.currentTimeMillis());
        long last = this.instanceContext.lastInstanceId();

        if (request.instanceId() <= last) {
            if (this.instanceContext.sameInHistory(request.instanceId(), request.ballot()) && (request == null)) {
                return new Event.AcceptResponse(config.serverId(), request.instanceId(), request.ballot(), true);
            }
            else {
                logger.error("AcceptResponse: historical in accept(instance id = {}), while my instance id is {} ",
                        request.instanceId(), last);
                return new Event.AcceptResponse(config.serverId(), request.instanceId(), Integer.MAX_VALUE, false);
            }
        }
        else if (request.instanceId() > last + 1) {
            logger.error("AcceptResponse: future in accept(instance id = {}), request instance id = {}", last, request.instanceId());
            return null;
        }
        else { // request.instanceId == last
            if (request.ballot() < this.maxBallot) {
                logger.info("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
                return new Event.AcceptResponse(config.serverId(), request.instanceId(), this.maxBallot, false);
            }
            else {
                if (request.ballot() > this.acceptedBallot) {
                    this.acceptedBallot = this.maxBallot = request.ballot();
                    this.acceptedValue = request.value();
                    logger.trace("Accept new value sender = {}, instance = {}, ballot = {}, value = {}",
                            request.senderId(), request.instanceId(), acceptedBallot, acceptedValue.toStringUtf8());

                    acceptorLogger.saveLastPromise(instanceContext.squadId(), request.instanceId(), this.maxBallot, this.acceptedValue);
                }
                return new Event.AcceptResponse(config.serverId(), request.instanceId(), this.maxBallot, true);
            }
        }
    }

    public synchronized void chose(Event.ChosenNotify notify) {
        logger.trace("receive chose notify {}", notify);
        long last = this.instanceContext.lastInstanceId();
        if (notify.instanceId() != last + 1) {
            logger.trace("unmatched instance id in chose notify({}), while my last instance id is {} ", notify.instanceId(), last);
            return;
        }

        instanceContext.learnValue(notify.instanceId(), notify.ballot(), this.acceptedValue);

        this.maxBallot = 0;
        this.acceptedValue = ByteString.EMPTY;
        this.acceptedBallot = 0;
    }
}
