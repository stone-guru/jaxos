package org.jaxos.network.protobuff;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.jaxos.JaxosConfig;
import org.jaxos.algo.Event;
import org.jaxos.network.CodingException;
import org.jaxos.network.MessageCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
                .build();
        encodeMap = decodeMap.inverse();
        this.config = config;
    }

    @Override
    public PaxosMessage.DataGram encode(Event event) {
        ByteString body;
        switch (event.code()) {
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
            default: {
                throw new UnsupportedOperationException();
            }
        }

        if(logger.isDebugEnabled()) {
            logger.debug("encode {}", event);
        }

        return PaxosMessage.DataGram.newBuilder()
                .setSender(event.senderId())
                .setCode(toProtoCode(event.code()))
                .setInstanceId(event.instanceId())
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
                .setSuccess(resp.success())
                .setMaxBallot(resp.maxBallot())
                .setAcceptedBallot(resp.acceptedBallot())
                .setAcceptedValue(ByteString.copyFrom(resp.acceptedValue()))
                .build()
                .toByteString();

    }

    private ByteString encodeBody(Event.AcceptRequest req) {
        return PaxosMessage.AcceptReq.newBuilder()
                .setBallot(req.ballot())
                .setValue(ByteString.copyFrom(req.value()))
                .build()
                .toByteString();
    }

    private ByteString encodeBody(Event.AcceptResponse resp) {
        return PaxosMessage.AcceptRes.newBuilder()
                .setMaxBallot(resp.maxBallot())
                .setSuccess(resp.accepted())
                .build()
                .toByteString();
    }

    private PaxosMessage.Code toProtoCode(Event.Code code) {
        return encodeMap.get(code);
    }


    @Override
    public Event decode(PaxosMessage.DataGram dataGram) {
        try {
            switch (dataGram.getCode()) {
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
        return new Event.PrepareRequest(dataGram.getSender(), dataGram.getInstanceId(), prepareReq.getBallot());
    }

    private Event decodePrepareResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.PrepareRes res = PaxosMessage.PrepareRes.parseFrom(dataGram.getBody());
        return new Event.PrepareResponse(dataGram.getSender(), dataGram.getInstanceId(),
                res.getSuccess(), res.getMaxBallot(), res.getAcceptedBallot(), res.getAcceptedValue().toByteArray());
    }

    private Event decodeAcceptReq(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptReq req = PaxosMessage.AcceptReq.parseFrom(dataGram.getBody());
        return new Event.AcceptRequest(dataGram.getSender(), dataGram.getInstanceId(), req.getBallot(), req.getValue().toByteArray());
    }

    private Event decodeAcceptResponse(PaxosMessage.DataGram dataGram) throws InvalidProtocolBufferException {
        PaxosMessage.AcceptRes res = PaxosMessage.AcceptRes.parseFrom(dataGram.getBody());
        return new Event.AcceptResponse(dataGram.getSender(), dataGram.getInstanceId(), res.getMaxBallot(), res.getSuccess());
    }
}
