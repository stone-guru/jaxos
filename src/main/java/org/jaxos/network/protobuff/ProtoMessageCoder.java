package org.jaxos.network.protobuff;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.jaxos.JaxosConfig;
import org.jaxos.algo.Event;
import org.jaxos.network.CodingException;
import org.jaxos.network.MessageCoder;

import java.util.Map;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ProtoMessageCoder implements MessageCoder<PaxosMessage.DataGram> {
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
            default: {
                throw new UnsupportedOperationException();
            }
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

    private PaxosMessage.Code toProtoCode(Event.Code code) {
        return encodeMap.get(code);
    }


    @Override
    public Event decode(PaxosMessage.DataGram dataGram) {
        switch (dataGram.getCode()) {
            case PREPARE_REQ: {
                return decodePrepareReq(dataGram);
            }
            case PREPARE_RES: {
                return decodePrepareResponse(dataGram);
            }
            default: {
                throw new UnsupportedOperationException();
            }
        }
    }

    private Event.Code toEventCode(PaxosMessage.Code code) {
        return decodeMap.get(code);
    }

    private Event decodePrepareReq(PaxosMessage.DataGram dataGram) {
        PaxosMessage.PrepareReq prepareReq = null;
        try {
            prepareReq = PaxosMessage.PrepareReq.parseFrom(dataGram.getBody());
        }
        catch (InvalidProtocolBufferException e) {
            throw new CodingException(e);
        }

        return new Event.PrepareRequest(dataGram.getSender(), dataGram.getInstanceId(), prepareReq.getBallot());
    }

    private Event decodePrepareResponse(PaxosMessage.DataGram dataGram) {
        PaxosMessage.PrepareRes res;
        try {
            res = PaxosMessage.PrepareRes.parseFrom(dataGram.getBody());
        }
        catch (InvalidProtocolBufferException e) {
            throw new CodingException(e);
        }

        return new Event.PrepareResponse(dataGram.getSender(), dataGram.getInstanceId(),
                res.getSuccess(), res.getMaxBallot(), res.getAcceptedBallot(), res.getAcceptedValue().toByteArray());
    }
}
