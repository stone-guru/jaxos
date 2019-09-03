package org.jaxos.algo;

import com.google.protobuf.ByteString;
import org.jaxos.JaxosConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Acceptor {
    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);
    public static final byte[] EMPTY_BYTE = new byte[0];

    public static class ProposalInfo {
        private final int serverId;
        private final long millis;

        public ProposalInfo(int serverId, long millis) {
            this.serverId = serverId;
            this.millis = millis;
        }

        public int serverId() {
            return this.serverId;
        }

        public long millis() {
            return this.millis;
        }
    }

    private volatile long instanceId;
    private AtomicInteger maxBallot;
    private AtomicReference<AcceptedValue> acceptedValue;
    private InstanceContext instanceContext;
    private JaxosConfig config;
    private AtomicReference<ProposalInfo> lastProposeInfo;

    public Acceptor(JaxosConfig config, InstanceContext instanceContext) {
        this.config = config;
        this.maxBallot = new AtomicInteger(0);
        this.acceptedValue = new AtomicReference<>(AcceptedValue.NONE);
        this.instanceContext = instanceContext;
        this.lastProposeInfo = new AtomicReference<>(new ProposalInfo(-1, 0));
    }

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        logger.debug("do prepare {} ", request);
        long last = this.instanceContext.lastInstanceId();

        if (request.instanceId() > last + 1) {
            logger.warn("future instance id in prepare(instance id = {}), while my last instance id is {} ",
                    request.instanceId(), last);
            return new Event.PrepareResponse(config.serverId(), last, false, 0, 0, ByteString.EMPTY);
        }
        else if (request.instanceId() < last) {
            logger.warn("historical instance id in prepare(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            ByteString v = instanceContext.valueOf(request.instanceId());
            return new Event.PrepareResponse(config.serverId(), request.instanceId(), false, 0, Integer.MAX_VALUE, v);
        }
        else if (request.instanceId() == last + 1) {
            //It's ok to start a new round
            if (this.instanceId == 0) {
                logger.info("start a new round with instance id {}", request.instanceId());
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

        AcceptedValue v = this.acceptedValue.get();
        return new Event.PrepareResponse(config.serverId(), this.instanceId, b0 <= request.ballot(), b0, v.ballot, v.content);
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request) {
        ProposalInfo info = this.lastProposeInfo.get();
        //it's the current leader
        if (info.serverId() == request.senderId()) {
            //FIXME request may curry wrong instance it
            this.instanceId = request.instanceId();
        }
        else {
            if (this.instanceId != request.instanceId()) {
                logger.warn("unmatched instance id in accept(instance id = {}), while my instance id is {} ",
                        request.instanceId(), this.instanceId);
                return null;
            }
        }

        boolean accepted = false;
        AcceptedValue v = null;
        for (; ; ) {
            int m0 = this.maxBallot.get();
            AcceptedValue v0 = this.acceptedValue.get();

            if (request.ballot() < m0) {
                logger.info("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
                break;
            }

            if (!this.maxBallot.compareAndSet(m0, request.ballot())) {
                continue;
            }

            AcceptedValue v1 = v != null ? v : new AcceptedValue(request.ballot(), request.value());
            v = v1;
            if (!this.acceptedValue.compareAndSet(v0, v1)) {
                continue;
            }
            accepted = true;
            this.lastProposeInfo.set(new ProposalInfo(request.senderId(), System.currentTimeMillis()));
            logger.info("Accept new value sender = {}, ballot = {}, value = {}", request.senderId(), v.ballot, v.content.toStringUtf8());
            break;
        }

        return new Event.AcceptResponse(config.serverId(), this.instanceId, this.maxBallot.get(), accepted);
    }

    public void chose(Event.ChosenNotify notify) {
        logger.debug("receive chose notify {}", notify);
        if (notify.instanceId() != this.instanceId) {
            logger.debug("unmatched instance id in chose notify({}), while my instance id is {} ", notify.instanceId(), this.instanceId);
            return;
        }

        AcceptedValue v0;
        v0 = this.acceptedValue.get();
        instanceContext.learnValue(this.instanceId, v0.content);

        this.acceptedValue.set(AcceptedValue.NONE);
        this.instanceId = 0;
    }
}
