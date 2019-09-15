package org.axesoft.jaxos.network.protobuff;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.JaxosConfig;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.network.CodingException;
import org.axesoft.jaxos.network.MessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ProtoMessageCoder implements MessageCoder<PaxosMessage.DataGram> {
    private static Logger logger = LoggerFactory.getLogger(ProtoMessageCoder.class);

    private BiMap<PaxosMessage.Code, Event.Code> decodeMap;
    private Map<Event.Code, PaxosMessage.Code> encodeMap;

    private JaxosConfig config;


    public ProtoMessageCoder(JaxosConfig config) {
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
                .build();
        encodeMap = decodeMap.inverse();
        this.config = config;
    }

    @Override
    public PaxosMessage.DataGram encode(Event event) {
        ByteString body;
        switch (event.code()) {
            case HEART_BEAT:
            case HEART_BEAT_RESPONSE:{
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
                byte[] bytes = Ints.toByteArray(((Event.ChosenNotify)event).ballot());
                body = ByteString.copyFrom(bytes);
                break;
            }
            default: {
                throw new UnsupportedOperationException();
            }
        }

        if(logger.isDebugEnabled()) {
            logger.trace("encode {}", event);
        }

        return PaxosMessage.DataGram.newBuilder()
                .setSender(event.senderId())
                .setCode(toProtoCode(event.code()))
                .setSquadId(event.squadId())
                .setInstanceId(event.instanceId())
                .setRound(event.round())
                .setBody(body)
                .build();
    }

    private ByteString encodeBody(Event.PrepareRequest req) {
        return PaxosMessage.PrepareReq.newBuilder()
                .setBallot(req.ballot())
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.PrepareResponse resp) {
        return PaxosMessage.PrepareRes.newBuilder()
                .setResult(resp.result())
                .setMaxBallot(resp.maxBallot())
                .setAcceptedBallot(resp.acceptedBallot())
                .setAcceptedValue(resp.acceptedValue())
                .setChosenInstanceId(resp.chosenInstanceId())
                .build()
                .toByteString();

    }

    private ByteString encodeBody(Event.AcceptRequest req) {
        return PaxosMessage.AcceptReq.newBuilder()
                .setBallot(req.ballot())
                .setValue(req.value())
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.AcceptResponse resp) {
        return PaxosMessage.AcceptRes.newBuilder()
                .setMaxBallot(resp.maxBallot())
                .setResult(resp.result())
                .setChosenInstanceId(resp.chosenInstanceId())
                .build()
                .toByteString();
    }

    private PaxosMessage.Code toProtoCode(Event.Code code) {
        return checkNotNull(this.encodeMap.get(code));
    }

    @Override
    public Event decode(PaxosMessage.DataGram dataGram) {
        try {
            switch (dataGram.getCode()) {
                case HEARTBEAT_REQ:{
                    return new Event.HeartBeatRequest(dataGram.getSender());
                }
                case HEARTBEAT_RES: {
                    return new Event.HeartBeatResponse(dataGram.getSender());
                }
                case PREPARE_REQ: {
                    return decodePrepareReq(dataGram);
                }
                case PREPARE_RES: {
                    return decodePrepareResponse(dataGram);
                }
                case ACCEPT_REQ:{
                    return decodeAcceptReq(dataGram);
                }
                case ACCEPT_RES:{
                    return decodeAcceptResponse(dataGram);
                }
                case ACCEPTED_NOTIFY: {
                    return decodeAcceptedNotify(dataGram);
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

    private Event decodePrepareReq(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.PrepareReq prepareReq = PaxosMessage.PrepareReq.parseFrom(dataGram.getBody());
        return new Event.PrepareRequest(dataGram.getSender(), dataGram.getSquadId(), dataGram.getInstanceId(), dataGram.getRound(), prepareReq.getBallot());
    }

    private Event decodePrepareResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.PrepareRes res = PaxosMessage.PrepareRes.parseFrom(dataGram.getBody());
        return new Event.PrepareResponse.Builder(dataGram.getSender(), dataGram.getSquadId(), dataGram.getInstanceId(), dataGram.getRound())
                .setResult(res.getResult())
                .setAccepted(res.getAcceptedBallot(), res.getAcceptedValue())
                .setMaxProposal(res.getMaxBallot())
                .setChosenInstanceId(res.getChosenInstanceId())
                .build();
    }

    private Event decodeAcceptReq(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptReq req = PaxosMessage.AcceptReq.parseFrom(dataGram.getBody());
        return new Event.AcceptRequest(dataGram.getSender(), dataGram.getSquadId(), dataGram.getInstanceId(), dataGram.getRound(), req.getBallot(), req.getValue());
    }

    private Event decodeAcceptResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptRes res = PaxosMessage.AcceptRes.parseFrom(dataGram.getBody());
        return new Event.AcceptResponse(dataGram.getSender(), dataGram.getSquadId(), dataGram.getInstanceId(), dataGram.getRound(), res.getMaxBallot(), res.getResult(), res.getChosenInstanceId());
    }

    private Event decodeAcceptedNotify(PaxosMessage.DataGram dataGram){
        int ballot = Ints.fromByteArray(dataGram.getBody().toByteArray());
        return new Event.ChosenNotify(dataGram.getSender(), dataGram.getSquadId(), dataGram.getInstanceId(), ballot);
    }
}
