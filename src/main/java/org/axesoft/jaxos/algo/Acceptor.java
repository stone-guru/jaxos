package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.JaxosSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Acceptor {
    private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);

    private final SquadContext context;
    private final Learner learner;
    private final JaxosSettings settings;
    private final Configuration config;

    private long acceptedInstanceId;
    private int maxBallot;
    private int acceptedBallot;
    private ByteString acceptedValue;

    public Acceptor(JaxosSettings settings, Configuration config, SquadContext context, Learner learner) {
        this.settings = settings;
        this.config = config;
        this.context = context;
        this.learner = learner;

        this.maxBallot = 0;
        this.acceptedBallot = 0;
        this.acceptedValue = ByteString.EMPTY;
    }

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("On prepare {} ", request);
        }

        long last0 = this.learner.lastChosenInstanceId(this.context.squadId());

        long last = handleAcceptedNotifyLostMaybe(last0, request.instanceId(), request.lastChosenBallot());

        if (request.instanceId() <= last) {
            logger.debug("PrepareResponse: historic prepare(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            InstanceValue p = this.config.getLogger().loadPromise(this.context.squadId(), request.instanceId());
            int proposal = (p == null) ? Integer.MAX_VALUE : p.proposal;
            ByteString value = (p == null) ? ByteString.EMPTY : p.value;
            return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(Event.RESULT_REJECT)
                    .setMaxProposal(Integer.MAX_VALUE)
                    .setAccepted(proposal, value)
                    .setChosenInstanceId(last)
                    .build();
        }
        else if (request.instanceId() > last + 1) {
            logger.warn("PrepareResponse: future instance id in prepare(instance id = {}), request instance id = {}", last, request.instanceId());
            return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(Event.RESULT_STANDBY)
                    .setMaxProposal(0)
                    .setAccepted(0, ByteString.EMPTY)
                    .setChosenInstanceId(last)
                    .build();
        }
        else { // request.instanceId == last + 1
            boolean success = false;
            int b0 = this.maxBallot;
            if (request.ballot() > this.maxBallot) {
                this.maxBallot = request.ballot();
                success = true;
                this.config.getLogger().savePromise(this.context.squadId(), request.instanceId(), request.ballot(), this.acceptedValue);
            }

            return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(success ? Event.RESULT_SUCCESS : Event.RESULT_REJECT)
                    .setMaxProposal(b0)
                    .setAccepted(this.acceptedBallot, this.acceptedValue)
                    .setChosenInstanceId(last)
                    .build();
        }
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request) {
        logger.trace("On Accept {}", request);

        long last0 = this.learner.lastChosenInstanceId(this.context.squadId());

        long last = handleAcceptedNotifyLostMaybe(last0, request.instanceId(), request.lastChosenBallot());

        if (request.instanceId() <= last) {
            logger.debug("AcceptResponse: historical in accept(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            return buildAcceptResponse(request, Integer.MAX_VALUE, Event.RESULT_REJECT);
        }
        else if (request.instanceId() > last + 1) {
            if (acceptedInstanceId == 0 || acceptedInstanceId == request.instanceId()) {
                acceptValueOptional(request);
            }
            logger.debug("AcceptResponse: future in accept(instance id = {}), request instance id = {}", last, request.instanceId());
            return buildAcceptResponse(request, 0, Event.RESULT_STANDBY);
        }
        else { // request.instanceId == last
            this.acceptedInstanceId = request.instanceId();
            if (request.ballot() < this.maxBallot) {
                logger.debug("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_REJECT);
            }
            else {
                acceptValueOptional(request);
                logger.trace("Accept new value sender = {}, instance = {}, ballot = {}, value = BX[{}]",
                        request.senderId(), request.instanceId(), acceptedBallot, acceptedValue.size());
                context.setPrepareSuccessRecord(request.senderId(), request.ballot());
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_SUCCESS);
            }
        }
    }

    private void acceptValueOptional(Event.AcceptRequest request) {
        if (request.ballot() > this.acceptedBallot) {
            this.acceptedInstanceId = request.instanceId();
            this.acceptedBallot = this.maxBallot = request.ballot();
            this.acceptedValue = request.value();
            this.config.getLogger().savePromise(this.context.squadId(), request.instanceId(), this.maxBallot, this.acceptedValue);
        }
    }

    private Event.AcceptResponse buildAcceptResponse(Event.AcceptRequest request, int proposal, int result) {
        return new Event.AcceptResponse(settings.serverId(), this.context.squadId(), request.instanceId(), request.round(),
                proposal, result, this.learner.lastChosenInstanceId(this.context.squadId()));
    }

    private long handleAcceptedNotifyLostMaybe(long lastInstanceId, long requestInstanceId, int lastChosenBallot) {
        if (requestInstanceId == lastInstanceId + 2) {
            if (this.acceptedBallot > 0 && lastChosenBallot == this.acceptedBallot) {
                logger.info("success handle notify lost, when handle prepare({}), mine is {}", requestInstanceId, lastInstanceId);
                chose(lastInstanceId, lastChosenBallot);
                return lastInstanceId + 1;
            }
        }
        return lastInstanceId;
    }

    public void onChosenNotify(Event.ChosenNotify notify) {
        if (logger.isTraceEnabled()) {
            logger.trace("NOTIFY: receive chose notify {}, value = {}", notify, valueToString(this.acceptedValue));
        }
        long last = this.learner.lastChosenInstanceId(this.context.squadId());
        if (notify.instanceId() == last + 1) {
            chose(notify.instanceId(), notify.ballot());
        }
        else if (notify.instanceId() <= last + 1) {
            logger.debug("NOTIFY: late notify message of chose notify({}), while my last instance id is {} ", notify.instanceId(), last);
        }
        else if (notify.instanceId() == this.acceptedInstanceId && notify.ballot() == this.acceptedBallot) {
            logger.trace("NOTIFY: future notify message of chose notify({}), cache it ", notify.instanceId());
            learner.cacheChosenValue(context.squadId(), notify.instanceId(), this.acceptedBallot, this.acceptedValue);
        }
        else {
            logger.warn("NOTIFY: future notify message of chose notify({}), mine is {} ", notify.instanceId(), last);
        }
    }

    private void chose(long instanceId, int proposal) {
        learner.learnValue(this.context.squadId(), instanceId, proposal, this.acceptedValue);

        // for multi paxos, prepare once and accept many, keep maxBallot unchanged
        this.acceptedBallot = 0;
        this.acceptedValue = ByteString.EMPTY;
    }

    private String valueToString(ByteString value) {
        if (settings.valueVerboser() != null) {
            try {
                return settings.valueVerboser().apply(value);
            }
            catch (Exception e) {
                //ignore
            }
        }
        return "bx[" + value.size() + "]";
    }

    public long lastChosenInstanceId() {
        return learner.lastChosenInstanceId(context.squadId());
    }
}
