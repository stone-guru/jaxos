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

    private volatile int maxBallot;
    private volatile int acceptedBallot;
    private volatile ByteString acceptedValue;
    private final int squadId;
    private Learner learner;
    private JaxosSettings config;
    private AcceptorLogger acceptorLogger;

    public Acceptor(JaxosSettings config, int squadId, Learner learner, AcceptorLogger acceptorLogger) {
        this.squadId = squadId;
        this.config = config;
        this.learner = learner;
        this.acceptorLogger = acceptorLogger;

        this.maxBallot = 0;
        this.acceptedValue = ByteString.EMPTY;

        AcceptorLogger.Promise promise = this.acceptorLogger.loadLastPromise(this.squadId);
        if (promise != null) {
            logger.info("Acceptor restore last instance {}", promise.instanceId);
            learner.learnLastChosenInstanceId(promise.instanceId);
        }
    }

    public synchronized Event.PrepareResponse prepare(Event.PrepareRequest request) {
        logger.trace("On prepare {} ", request);

        long last = this.learner.lastChosenInstanceId();

        if (request.instanceId() <= last) {
            logger.warn("PrepareResponse: historic prepare(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            AcceptorLogger.Promise p = this.acceptorLogger.loadPromise(this.squadId, request.instanceId());
            int proposal = (p == null)? Integer.MAX_VALUE : p.proposal;
            ByteString value = (p == null)? ByteString.EMPTY : p.value;
            return new Event.PrepareResponse.Builder(config.serverId(), this.squadId , request.instanceId(), request.round())
                    .setResult(Event.RESULT_REJECT)
                    .setMaxProposal(Integer.MAX_VALUE)
                    .setAccepted(proposal, value)
                    .setChosenInstanceId(last)
                    .build();
        }
        else if (request.instanceId() > last + 1) {
            logger.error("PrepareResponse: future instance id in prepare(instance id = {}), request instance id = {}", last, request.instanceId());
            return new Event.PrepareResponse.Builder(config.serverId(), this.squadId , request.instanceId(), request.round())
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
                acceptorLogger.savePromise(this.squadId , request.instanceId(), request.ballot(), this.acceptedValue);
            }

            return new Event.PrepareResponse.Builder(config.serverId(), this.squadId , request.instanceId(), request.round())
                    .setResult(success? Event.RESULT_SUCCESS : Event.RESULT_REJECT)
                    .setMaxProposal(b0)
                    .setAccepted(acceptedBallot, acceptedValue)
                    .setChosenInstanceId(last)
                    .build();
        }
    }

    public synchronized Event.AcceptResponse accept(Event.AcceptRequest request) {
        logger.trace("On Accept {}", request);

        long last = this.learner.lastChosenInstanceId();

        if (request.instanceId() <= last) {
            logger.error("AcceptResponse: historical in accept(instance id = {}), while my instance id is {} ",
                    request.instanceId(), last);
            return buildAcceptResponse(request, Integer.MAX_VALUE, Event.RESULT_REJECT);
        }
        else if (request.instanceId() > last + 1) {
            logger.error("AcceptResponse: future in accept(instance id = {}), request instance id = {}", last, request.instanceId());
            return buildAcceptResponse(request, 0, Event.RESULT_STANDBY);
        }
        else { // request.instanceId == last
            if (request.ballot() < this.maxBallot) {
                logger.info("Reject accept ballot = {}, while my maxBallot={}", request.ballot(), this.maxBallot);
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_REJECT);
            }
            else {
                if (request.ballot() > this.acceptedBallot) {
                    this.acceptedBallot = this.maxBallot = request.ballot();
                    this.acceptedValue = request.value();
                    logger.trace("Accept new value sender = {}, instance = {}, ballot = {}, value = BX[{}]",
                            request.senderId(), request.instanceId(), acceptedBallot, acceptedValue.size());

                    acceptorLogger.savePromise(this.squadId, request.instanceId(), this.maxBallot, this.acceptedValue);
                }
                return buildAcceptResponse(request, this.maxBallot, Event.RESULT_SUCCESS);
            }
        }
    }

    private Event.AcceptResponse buildAcceptResponse(Event.AcceptRequest request, int proposal, int result) {
        return new Event.AcceptResponse(config.serverId(), this.squadId, request.instanceId(), request.round(),
                proposal, result, this.learner.lastChosenInstanceId());
    }

    public synchronized void chose(Event.ChosenNotify notify) {
        if(logger.isTraceEnabled()) {
            logger.trace("receive chose notify {}, value = {}", notify, valueToString(this.acceptedValue));
        }
        long last = this.learner.lastChosenInstanceId();
        if (notify.instanceId() != last + 1) {
            logger.warn("unmatched instance id in chose notify({}), while my last instance id is {} ", notify.instanceId(), last);
            return;
        }

        learner.learnValue(notify.instanceId(), notify.ballot(), this.acceptedValue);

        this.maxBallot = 0;
        this.acceptedValue = ByteString.EMPTY;
        this.acceptedBallot = 0;
    }

    private String valueToString(ByteString value){
        if(config.valueVerboser() != null){
            try{
                return config.valueVerboser().apply(value);
            }catch (Exception e){
                //ignore
            }
        }
        return "BX[" + value.size() + "]";
    }
}
