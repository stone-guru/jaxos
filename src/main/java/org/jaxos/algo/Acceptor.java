package org.jaxos.algo;

import com.google.protobuf.ByteString;
import org.jaxos.JaxosConfig;
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
    private AtomicInteger maxBallot;
    private AtomicReference<ValueWithProposal> acceptedValue;
    private InstanceContext instanceContext;
    private JaxosConfig config;

    public Acceptor(JaxosConfig config, InstanceContext instanceContext) {
        this.config = config;
        this.maxBallot = new AtomicInteger(0);
        this.acceptedValue = new AtomicReference<>(ValueWithProposal.NONE);
        this.instanceContext = instanceContext;

    }

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
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

        int b0;
        boolean again;
        do {
            b0 = this.maxBallot.get();
            if (request.ballot() > b0) {
                again = !this.maxBallot.compareAndSet(b0, request.ballot());
            }
            else {
                again = false;
            }
        } while (again);

        ValueWithProposal v = this.acceptedValue.get();
        return new Event.PrepareResponse(config.serverId(), this.instanceId, b0 <= request.ballot(), b0, v.ballot, v.content);
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request) {
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
                return null;
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

        boolean accepted = false;
        ValueWithProposal v = null;
        for (; ; ) {
            int m0 = this.maxBallot.get();
            ValueWithProposal v0 = this.acceptedValue.get();

            if (request.ballot() < m0) {
                logger.info("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
                break;
            }

            if (!this.maxBallot.compareAndSet(m0, request.ballot())) {
                continue;
            }

            ValueWithProposal v1 = v != null ? v : new ValueWithProposal(request.ballot(), request.value());
            v = v1;
            if (!this.acceptedValue.compareAndSet(v0, v1)) {
                continue;
            }
            accepted = true;

            logger.debug("Accept new value sender = {}, ballot = {}, value = {}", request.senderId(), v.ballot, v.content.toStringUtf8());
            break;
        }

        this.instanceContext.recordLastRequest(request.senderId(), System.currentTimeMillis());
        return new Event.AcceptResponse(config.serverId(), this.instanceId, this.maxBallot.get(), accepted);
    }

    public void chose(Event.ChosenNotify notify) {
        logger.debug("receive chose notify {}", notify);
        if (notify.instanceId() != this.instanceId) {
            logger.debug("unmatched instance id in chose notify({}), while my instance id is {} ", notify.instanceId(), this.instanceId);
            return;
        }

        ValueWithProposal v0;
        v0 = this.acceptedValue.get();
        instanceContext.learnValue(this.instanceId, v0.content);

        this.acceptedValue.set(ValueWithProposal.NONE);
        this.maxBallot.set(0);
        this.instanceId = 0;
    }
}
