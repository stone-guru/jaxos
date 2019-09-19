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
    private final JaxosSettings config;
    private final AcceptorLogger acceptorLogger;

    private int maxBallot;
    private int acceptedBallot;
    private ByteString acceptedValue;

    public Acceptor(JaxosSettings config, SquadContext context, Learner learner, AcceptorLogger acceptorLogger) {
        this.context = context;
        this.config = config;
        this.learner = learner;
        this.acceptorLogger = acceptorLogger;

        this.maxBallot = 0;
        this.acceptedBallot = 0;
        this.acceptedValue = ByteString.EMPTY;

        restoreLastProposal(learner);
    }

    private void restoreLastProposal(Learner learner) {
        AcceptorLogger.Promise promise = this.acceptorLogger.loadLastPromise(this.context.squadId());
        if (promise == null) {
            promise = new AcceptorLogger.Promise(this.context.squadId(), 0, 0, ByteString.EMPTY);
        }

        logger.info("Acceptor restore squad {}, instance {}, proposal {}", promise.squadId, promise.instanceId, promise.proposal);
        learner.learnLastChosen(promise.squadId, promise.instanceId, promise.proposal);
    }

    public Event.PrepareResponse prepare(Event.PrepareRequest request) {
        if(logger.isTraceEnabled()) {
            logger.trace("On prepare {} ", request);
        }

        long last = this.learner.lastChosenInstanceId(this.context.squadId());

        //maybe the chosen notify event lost for me
        if (request.instanceId() == last + 2) {
            if (this.acceptedBallot > 0 && request.lastChosenBallot() == this.acceptedBallot) {
                logger.info("PrepareResponse: success handle notify lost, when handle prepare({}), mine is {}", request.instanceId(), last);
                last = last + 1;
                chose(last, request.lastChosenBallot());
            }
        }

        if (request.instanceId() <= last) {
            logger.debug("PrepareResponse: historic prepare(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            AcceptorLogger.Promise p = this.acceptorLogger.loadPromise(this.context.squadId(), request.instanceId());
            int proposal = (p == null) ? Integer.MAX_VALUE : p.proposal;
            ByteString value = (p == null) ? ByteString.EMPTY : p.value;
            return new Event.PrepareResponse.Builder(config.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(Event.RESULT_REJECT)
                    .setMaxProposal(Integer.MAX_VALUE)
                    .setAccepted(proposal, value)
                    .setChosenInstanceId(last)
                    .build();
        }
        else if (request.instanceId() > last + 1) {
            logger.warn("PrepareResponse: future instance id in prepare(instance id = {}), request instance id = {}", last, request.instanceId());
            return new Event.PrepareResponse.Builder(config.serverId(), this.context.squadId(), request.instanceId(), request.round())
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
                this.acceptorLogger.savePromise(this.context.squadId(), request.instanceId(), request.ballot(), this.acceptedValue);
            }

            return new Event.PrepareResponse.Builder(config.serverId(), this.context.squadId(), request.instanceId(), request.round())
                    .setResult(success ? Event.RESULT_SUCCESS : Event.RESULT_REJECT)
                    .setMaxProposal(b0)
                    .setAccepted(this.acceptedBallot, this.acceptedValue)
                    .setChosenInstanceId(last)
                    .build();
        }
    }

    public Event.AcceptResponse accept(Event.AcceptRequest request) {
        logger.trace("On Accept {}", request);

        long last = this.learner.lastChosenInstanceId(this.context.squadId());

        if (request.instanceId() <= last) {
            logger.debug("AcceptResponse: historical in accept(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            return buildAcceptResponse(request, Integer.MAX_VALUE, Event.RESULT_REJECT);
        }
        else if (request.instanceId() > last + 1) {
            logger.debug("AcceptResponse: future in accept(instance id = {}), request instance id = {}", last, request.instanceId());
            return buildAcceptResponse(request, 0, Event.RESULT_STANDBY);
        }
        else { // request.instanceId == last
            if (request.ballot() < this.maxBallot) {
                logger.debug("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_REJECT);
            }
            else {
                if (request.ballot() > this.acceptedBallot) {
                    this.acceptedBallot = this.maxBallot = request.ballot();
                    this.acceptedValue = request.value();
                    logger.trace("Accept new value sender = {}, instance = {}, ballot = {}, value = BX[{}]",
                            request.senderId(), request.instanceId(), acceptedBallot, acceptedValue.size());

                    acceptorLogger.savePromise(this.context.squadId(), request.instanceId(), this.maxBallot, this.acceptedValue);
                }
                context.setPrepareSuccessRecord(request.senderId(), request.ballot());
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_SUCCESS);
            }
        }
    }

    private Event.AcceptResponse buildAcceptResponse(Event.AcceptRequest request, int proposal, int result) {
        return new Event.AcceptResponse(config.serverId(), this.context.squadId(), request.instanceId(), request.round(),
                proposal, result, this.learner.lastChosenInstanceId(this.context.squadId()));
    }

    public void onChoseNotify(Event.ChosenNotify notify) {
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
        else {
            logger.warn("NOTIFY: future notify message of chose notify({}), while my last instance id is {} ", notify.instanceId(), last);
        }
    }

    private void chose(long instanceId, int proposal) {
        learner.learnValue(this.context.squadId(), instanceId, proposal, this.acceptedValue);

        // for multi paxos, prepare once and accept many, keep maxBallot unchanged
        this.acceptedBallot = 0;
        this.acceptedValue = ByteString.EMPTY;
    }

    private String valueToString(ByteString value) {
        if (config.valueVerboser() != null) {
            try {
                return config.valueVerboser().apply(value);
            } catch (Exception e) {
                //ignore
            }
        }
        return "bx[" + value.size() + "]";
    }
}
