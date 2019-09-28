package org.axesoft.jaxos.network.protobuff;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.InstanceValue;
import org.axesoft.jaxos.network.CodingException;
import org.axesoft.jaxos.network.MessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ProtoMessageCoder implements MessageCoder<PaxosMessage.DataGram> {
    private static Logger logger = LoggerFactory.getLogger(ProtoMessageCoder.class);

    private BiMap<PaxosMessage.Code, Event.Code> decodeMap;
    private Map<Event.Code, PaxosMessage.Code> encodeMap;

    private JaxosSettings config;


    public ProtoMessageCoder(JaxosSettings config) {
        decodeMap = ImmutableBiMap.<PaxosMessage.Code, Event.Code>builder()
                .put(PaxosMessage.Code.NOOP, Event.Code.NOOP)
                .put(PaxosMessage.Code.HEARTBEAT_REQ, Event.Code.HEART_BEAT)
                .put(PaxosMessage.Code.HEARTBEAT_RES, Event.Code.HEART_BEAT_RESPONSE)
                .put(PaxosMessage.Code.ACCEPT_REQ, Event.Code.ACCEPT)
                .put(PaxosMessage.Code.ACCEPT_RES, Event.Code.ACCEPT_RESPONSE)
                .put(PaxosMessage.Code.PREPARE_REQ, Event.Code.PREPARE)
                .put(PaxosMessage.Code.PREPARE_RES, Event.Code.PREPARE_RESPONSE)
                .put(PaxosMessage.Code.ACCEPTED_NOTIFY, Event.Code.ACCEPTED_NOTIFY)
                .put(PaxosMessage.Code.ACCEPTED_ACK, Event.Code.ACCEPTED_NOTIFY_RESPONSE)
                .put(PaxosMessage.Code.LEARN_REQ, Event.Code.LEARN_REQUEST)
                .put(PaxosMessage.Code.LEARN_RES, Event.Code.LEARN_RESPONSE)
                .put(PaxosMessage.Code.CHOSEN_QUERY_REQ, Event.Code.CHOSEN_QUERY)
                .put(PaxosMessage.Code.CHOSEN_QUERY_RES, Event.Code.CHOSEN_QUERY_RESPONSE)
                .build();
        encodeMap = decodeMap.inverse();
        this.config = config;
    }

    @Override
    public PaxosMessage.DataGram encode(Event event) {
        ByteString body;
        switch (event.code()) {
            case HEART_BEAT:
            case HEART_BEAT_RESPONSE: {
                body = ByteString.EMPTY;
                break;
            }
            case PREPARE: {
                body = encodeBody((Event.PrepareRequest) event);
                break;
            }
            case PREPARE_RESPONSE: {
                body = encodeBody((Event.PrepareResponse) event);
                break;
            }
            case ACCEPT: {
                body = encodeBody((Event.AcceptRequest) event);
                break;
            }
            case ACCEPT_RESPONSE: {
                body = encodeBody((Event.AcceptResponse) event);
                break;
            }
            case ACCEPTED_NOTIFY: {
                body = encodeBody((Event.ChosenNotify) event);
                break;
            }
            case LEARN_REQUEST: {
                body = encodeBody((Event.Learn) event);
                break;
            }
            case LEARN_RESPONSE: {
                body = encodeBody((Event.LearnResponse) event);
                break;
            }
            case CHOSEN_QUERY: {
                body = ByteString.EMPTY;
                break;
            }
            case CHOSEN_QUERY_RESPONSE: {
                body = encodeBody((Event.ChosenQueryResponse) event);
                break;
            }
            default: {
                throw new UnsupportedOperationException();
            }
        }

        if (logger.isDebugEnabled()) {
            logger.trace("encode {}", event);
        }

        return PaxosMessage.DataGram.newBuilder()
                .setSender(event.senderId())
                .setTimestamp(event.timestamp())
                .setCode(toProtoCode(event.code()))
                .setBody(body)
                .build();
    }

    private ByteString encodeBody(Event.ChosenQueryResponse event) {
        return PaxosMessage.ChosenQueryRes.newBuilder()
                .addAllChosen(event.squadChosen().stream()
                        .map(p -> PaxosMessage.SquadChosen.newBuilder()
                                .setSquadId(p.getKey())
                                .setInstanceId(p.getValue())
                                .build())
                        .collect(Collectors.toList()))
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.PrepareRequest req) {
        return PaxosMessage.PrepareReq.newBuilder()
                .setSquadId(req.squadId())
                .setInstanceId(req.instanceId())
                .setRound(req.round())
                .setProposal(req.ballot())
                .setLastChosenProposal(req.lastChosenBallot())
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.PrepareResponse resp) {
        return PaxosMessage.PrepareRes.newBuilder()
                .setSquadId(resp.squadId())
                .setInstanceId(resp.instanceId())
                .setRound(resp.round())
                .setResult(resp.result())
                .setMaxProposal(resp.maxBallot())
                .setAcceptedProposal(resp.acceptedBallot())
                .setAcceptedValue(resp.acceptedValue())
                .setChosenInstanceId(resp.chosenInstanceId())
                .build()
                .toByteString();

    }

    private ByteString encodeBody(Event.AcceptRequest req) {
        return PaxosMessage.AcceptReq.newBuilder()
                .setSquadId(req.squadId())
                .setInstanceId(req.instanceId())
                .setRound(req.round())
                .setProposal(req.ballot())
                .setValue(req.value())
                .setLastChosenProposal(req.lastChosenBallot())
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.AcceptResponse resp) {
        return PaxosMessage.AcceptRes.newBuilder()
                .setSquadId(resp.squadId())
                .setInstanceId(resp.instanceId())
                .setRound(resp.round())
                .setResult(resp.result())
                .setMaxProposal(resp.maxBallot())
                .setChosenInstanceId(resp.chosenInstanceId())
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.ChosenNotify notify) {
        return PaxosMessage.AcceptedNotify.newBuilder()
                .setSquadId(notify.squadId())
                .setInstanceId(notify.instanceId())
                .setProposal(notify.ballot())
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.Learn req) {
        return PaxosMessage.LearnReq.newBuilder()
                .setSquadId(req.squadId())
                .setLowInstanceId(req.lowInstanceId())
                .setHighInstanceId(req.highInstanceId())
                .build()
                .toByteString();
    }


    private ByteString encodeBody(Event.LearnResponse response) {
        PaxosMessage.LearnRes.Builder builder = PaxosMessage.LearnRes.newBuilder()
                .setSquadId(response.squadId());

        for (InstanceValue i : response.instances()) {
            builder.addInstanceValue(PaxosMessage.InstanceValue.newBuilder()
                    .setSquadId(i.squadId())
                    .setInstanceId(i.instanceId())
                    .setProposal(i.proposal())
                    .setValue(i.value()));
        }

        return builder.build().toByteString();
    }

    private PaxosMessage.Code toProtoCode(Event.Code code) {
        return checkNotNull(this.encodeMap.get(code));
    }

    @Override
    public Event decode(PaxosMessage.DataGram dataGram) {
        try {
            switch (dataGram.getCode()) {
                case HEARTBEAT_REQ: {
                    return new Event.HeartBeatRequest(dataGram.getSender(), dataGram.getTimestamp());
                }
                case HEARTBEAT_RES: {
                    return new Event.HeartBeatResponse(dataGram.getSender(), dataGram.getTimestamp());
                }
                case PREPARE_REQ: {
                    return decodePrepareReq(dataGram);
                }
                case PREPARE_RES: {
                    return decodePrepareResponse(dataGram);
                }
                case ACCEPT_REQ: {
                    return decodeAcceptReq(dataGram);
                }
                case ACCEPT_RES: {
                    return decodeAcceptResponse(dataGram);
                }
                case ACCEPTED_NOTIFY: {
                    return decodeAcceptedNotify(dataGram);
                }
                case LEARN_REQ: {
                    return decodeLearnReq(dataGram);
                }
                case LEARN_RES: {
                    return decodeLearnResponse(dataGram);
                }
                case CHOSEN_QUERY_REQ: {
                    return new Event.ChosenQuery(dataGram.getSender());
                }
                case CHOSEN_QUERY_RES: {
                    return decodeChosenQueryResponse(dataGram);
                }
                default: {
                    logger.error("Unknown dataGram {}", dataGram);
                    return null;
                }
            }
        }
        catch (InvalidProtocolBufferException e) {
            throw new CodingException(e);
        }
    }

    private Event decodeChosenQueryResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.ChosenQueryRes res = PaxosMessage.ChosenQueryRes.parseFrom(dataGram.getBody());
        return new Event.ChosenQueryResponse(dataGram.getSender(),
                res.getChosenList().stream()
                        .map(c -> Pair.of(c.getSquadId(), c.getInstanceId()))
                        .collect(Collectors.toList()));
    }

    private Event decodePrepareReq(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.PrepareReq req = PaxosMessage.PrepareReq.parseFrom(dataGram.getBody());
        return new Event.PrepareRequest(dataGram.getSender(), req.getSquadId(), req.getInstanceId(), req.getRound(),
                req.getProposal(), req.getLastChosenProposal());
    }

    private Event decodePrepareResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.PrepareRes res = PaxosMessage.PrepareRes.parseFrom(dataGram.getBody());
        return new Event.PrepareResponse.Builder(dataGram.getSender(), res.getSquadId(), res.getInstanceId(), res.getRound())
                .setResult(res.getResult())
                .setAccepted(res.getAcceptedProposal(), res.getAcceptedValue())
                .setMaxProposal(res.getMaxProposal())
                .setChosenInstanceId(res.getChosenInstanceId())
                .build();
    }

    private Event decodeAcceptReq(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptReq req = PaxosMessage.AcceptReq.parseFrom(dataGram.getBody());
        return new Event.AcceptRequest(dataGram.getSender(), req.getSquadId(),
                req.getInstanceId(), req.getRound(),
                req.getProposal(), req.getValue(),
                req.getLastChosenProposal());
    }

    private Event decodeAcceptResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptRes res = PaxosMessage.AcceptRes.parseFrom(dataGram.getBody());
        return new Event.AcceptResponse(dataGram.getSender(), res.getSquadId(),
                res.getInstanceId(), res.getRound(),
                res.getMaxProposal(), res.getResult(), res.getChosenInstanceId());
    }

    private Event decodeAcceptedNotify(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptedNotify notify = PaxosMessage.AcceptedNotify.parseFrom(dataGram.getBody());
        return new Event.ChosenNotify(dataGram.getSender(), notify.getSquadId(), notify.getInstanceId(), notify.getProposal());
    }

    private Event decodeLearnReq(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.LearnReq req = PaxosMessage.LearnReq.parseFrom(dataGram.getBody());
        return new Event.Learn(dataGram.getSender(), req.getSquadId(), req.getLowInstanceId(), req.getHighInstanceId());
    }

    private Event decodeLearnResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.LearnRes res = PaxosMessage.LearnRes.parseFrom(dataGram.getBody());

        List<InstanceValue> ix = res.getInstanceValueList().stream()
                .map(v -> new InstanceValue(v.getSquadId(), v.getInstanceId(), v.getProposal(), v.getValue()))
                .collect(Collectors.toList());

        return new Event.LearnResponse(dataGram.getSender(), res.getSquadId(), ix);
    }
}
