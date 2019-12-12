package org.axesoft.jaxos.algo;

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
    private final Components config;

    /**
     * Indicate that this acceptor has encountered an unrecoverable error and can not work any more
     */
    private boolean faulty;

    private long acceptedInstanceId;
    private int maxBallot;
    private int acceptedBallot;
    private Event.BallotValue acceptedValue;

    public Acceptor(JaxosSettings settings, Components config, SquadContext context, Learner learner) {
        this.settings = settings;
        this.config = config;
        this.context = context;
        this.learner = learner;

        this.faulty = false;

        this.maxBallot = 0;
        this.acceptedBallot = 0;
        this.acceptedValue = Event.BallotValue.EMPTY;
    }

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        if (logger.isTraceEnabled()) {
            logger.trace("S{}: On prepare {} ", context.squadId(), request);
        }

        if(this.faulty){
            return null;
        }

        Instance i1 = this.learner.getLastChosenInstance(this.context.squadId());
        long last = handleAcceptedNotifyLostMaybe(i1.instanceId(), request.instanceId(), request.lastChosenBallot());

        if (request.instanceId() <= last) {
            logger.debug("S{}: PrepareResponse: historic prepare(instance id = {}), while my instance id is {} ",
                    context.squadId(), request.instanceId(), last);
            return outdatedPrepareResponse(request, last);
        }
        else if (request.instanceId() > last + 1) {
            logger.warn("S{}: PrepareResponse: future instance id in prepare(instance id = {}), request instance id = {}",
                    context.squadId(), last, request.instanceId());
            return standByPrepareResponse(request, last);
        }
        else { // request.instanceId == last + 1
            boolean success = false;
            int b0 = this.maxBallot;
            if (request.ballot() > this.maxBallot) {
                this.maxBallot = request.ballot();
                success = true;
                this.config.getLogger().savePromise(this.context.squadId(), request.instanceId(), request.ballot(), this.acceptedValue);
            }

            if (!success && logger.isDebugEnabled()) {
                logger.debug("S{}: Reject prepare I {} ballot = {} while my max ballot = {}",
                        context.squadId(), request.instanceId(), request.ballot(), this.maxBallot);
            }

            return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(success ? Event.RESULT_SUCCESS : Event.RESULT_REJECT)
                    .setMaxProposal(b0)
                    .setAccepted(this.acceptedBallot, this.acceptedValue)
                    .setChosenInstanceId(last)
                    .build();
        }
    }

    private Event.PrepareResponse standByPrepareResponse(Event.PrepareRequest request, long chosenInstanceId) {
        return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                .setResult(Event.RESULT_STANDBY)
                .setMaxProposal(0)
                .setAccepted(0, Event.BallotValue.EMPTY)
                .setChosenInstanceId(chosenInstanceId)
                .build();
    }

    private Event.PrepareResponse outdatedPrepareResponse(Event.PrepareRequest request, long chosenInstanceId) {
        Instance i0 = this.config.getLogger().loadPromise(this.context.squadId(), request.instanceId());
        return new Event.PrepareResponse.Builder(settings.serverId(), this.context.squadId(), request.instanceId(), request.round())
                .setResult(Event.RESULT_REJECT)
                .setMaxProposal(Integer.MAX_VALUE)
                .setAccepted(i0.isEmpty()? Integer.MAX_VALUE : i0.proposal(), i0.value())
                .setChosenInstanceId(chosenInstanceId)
                .build();
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request) {
        if(logger.isTraceEnabled()) {
            logger.trace("On Accept {}", request);
        }
        if(faulty){
            return null;
        }

        Instance i1 = this.learner.getLastChosenInstance(this.context.squadId());
        long last = handleAcceptedNotifyLostMaybe(i1.instanceId(), request.instanceId(), request.lastChosenBallot());

        if (request.instanceId() <= last) {
            if (logger.isDebugEnabled()) {
                logger.debug("S{}: AcceptResponse: historical in accept(instance id = {}), while my instance id is {} ",
                        context.squadId(), request.instanceId(), last);
            }
            return buildAcceptResponse(request, Integer.MAX_VALUE, Event.RESULT_REJECT, i1.instanceId());
        }
        else if (request.instanceId() > last + 1) {
            if (acceptedInstanceId == 0 || acceptedInstanceId == request.instanceId()) {
                acceptValueOptional(request);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("S{}: AcceptResponse: future in accept(instance id = {}), request instance id = {}",
                        context.squadId(), last, request.instanceId());
            }

            return buildAcceptResponse(request, 0, Event.RESULT_STANDBY, i1.instanceId());
        }
        else { // request.instanceId == last
            this.acceptedInstanceId = request.instanceId();
            if (request.ballot() < this.maxBallot) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S{}: Reject accept {}  ballot = {}, while my maxBallot={}",
                            context.squadId(), request.instanceId(), request.ballot(), this.maxBallot);
                }
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_REJECT, i1.instanceId());
            }
            else {
                acceptValueOptional(request);
                if (logger.isTraceEnabled()) {
                    logger.trace("S{}: Accept new value sender = {}, instance = {}, ballot = {}, value = {}",
                            context.squadId(), request.senderId(), request.instanceId(), acceptedBallot, acceptedValue);
                }

                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_SUCCESS, i1.instanceId());
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

    private Event.AcceptResponse buildAcceptResponse(Event.AcceptRequest request, int proposal, int result, long lastChosenInstanceId) {
        return new Event.AcceptResponse(settings.serverId(), this.context.squadId(), request.instanceId(), request.round(),
                proposal, result, this.acceptedValue.id(), lastChosenInstanceId);
    }

    private long handleAcceptedNotifyLostMaybe(long lastInstanceId, long requestInstanceId, int lastChosenBallot) {
        if (requestInstanceId == lastInstanceId + 2) {
            if (this.acceptedBallot > 0 && lastChosenBallot == this.acceptedBallot) {
                logger.info("S{}: success handle notify lost, when handle prepare({}), mine is {}",
                        context.squadId(), requestInstanceId, lastInstanceId);

                chose(lastInstanceId, lastChosenBallot);
                return lastInstanceId + 1;
            }
        }
        return lastInstanceId;
    }

    public void onChosenNotify(Event.ChosenNotify notify) {
        if (logger.isTraceEnabled()) {
            logger.trace("S{}: NOTIFY receive chose notify {}, value = {}",
                    context.squadId(), notify, this.acceptedValue);
        }

        if(this.faulty){
            return;
        }

        long last = this.learner.getLastChosenInstance(this.context.squadId()).instanceId();
        if (notify.instanceId() == last + 1 ) {
            if(notify.ballotId() != this.acceptedValue.id()){
                logger.error("S{} I{} Got notify event with different ballot id {}, mine is {}  ", context.squadId(),
                        notify.instanceId(), notify.ballotId(), this.acceptedValue.id());
                this.faulty = true;
                return;
            }
            chose(notify.instanceId(), notify.ballot());
            context.setAcceptSuccessRecord(notify.senderId(), notify.ballot());
        }
        else if (notify.instanceId() <= last + 1) {
            logger.debug("S{}: NOTIFY late notify message of chose notify({}), while my last instance id is {} ",
                    context.squadId(), notify.instanceId(), last);
        }
        else if (notify.instanceId() == this.acceptedInstanceId && notify.ballot() == this.acceptedBallot) {
            if (logger.isTraceEnabled()) {
                logger.trace("S{}: NOTIFY future notify message of chose notify({}) mine is {}, cache it ",
                        context.squadId(), notify.instanceId(), last);
            }

            //learner.cacheChosenValue(context.squadId(), notify.instanceId(), this.acceptedBallot, this.acceptedValue);
        }
        else {
            logger.warn("S{}: NOTIFY: future notify message of chose notify({}), mine is {} ",
                    context.squadId(), notify.instanceId(), last);
        }
    }

    private void chose(long instanceId, int proposal) {
        try {
            learner.learnValue(new Instance(this.context.squadId(), instanceId, proposal, this.acceptedValue));
        }catch(Exception e){
            this.faulty = true;

            String msg = String.format("Error when chosen value %d.%d", this.context.squadId(), instanceId);
            logger.error(msg, e);
        }

        // for multi paxos, prepare once and accept many, keep maxBallot unchanged
        // this.maxBallot = unchanged
        this.acceptedBallot = 0;
        this.acceptedValue = Event.BallotValue.EMPTY;
    }

    public long lastChosenInstanceId() {
        return learner.getLastChosenInstance(context.squadId()).instanceId();
    }
}
