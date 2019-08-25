package org.jaxos.network.protobuff;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.tuple.Pair;
import org.jaxos.algo.Acceptor;
import org.jaxos.algo.Message;
import org.jaxos.network.RequestDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class ProtoRequestDispatcher implements RequestDispatcher {

    public interface RequestHandler {
        Pair<PaxosMessage.Code, ByteString> process(PaxosMessage.DataGram request) throws Exception;
    }

    private static Logger logger = LoggerFactory.getLogger(ProtoRequestDispatcher.class);

    private Acceptor acceptor;
    private int serverId;

    private Map<PaxosMessage.Code, RequestHandler> handlerMap = prepareHandlerMap();

    private Map<PaxosMessage.Code, RequestHandler> prepareHandlerMap() {
        return ImmutableMap.<PaxosMessage.Code, RequestHandler>builder()
                .put(PaxosMessage.Code.PREPARE_REQ, new PrepareRequestHandler())
                .build();
    }

    private RequestHandler defaultHandler = dataGram -> {
        logger.warn("unknown request code" + dataGram.getCode());
        return null;
    };

    public ProtoRequestDispatcher(int serverId, Acceptor acceptor) {
        this.acceptor = acceptor;
        this.serverId = serverId;
    }

    @Override
    public PaxosMessage.DataGram process(PaxosMessage.DataGram request) {
        try {
            RequestHandler handler = handlerMap.getOrDefault(request.getCode(), this.defaultHandler);
            Pair<PaxosMessage.Code, ByteString> result = handler.process(request);
            if (result == null) {
                return null;
            }

            return PaxosMessage.DataGram.newBuilder()
                    .setSender(this.serverId)
                    .setCode(result.getKey())
                    .setInstanceId(request.getInstanceId())
                    .setBody(result.getValue())
                    .build();
        }
        catch (Exception e) {
            logger.error("invalid proto data", e);
            return null;
        }
    }

    private class PrepareRequestHandler implements RequestHandler {
        @Override
        public Pair<PaxosMessage.Code, ByteString> process(PaxosMessage.DataGram request) throws Exception {
            PaxosMessage.PrepareReq prepareReq = PaxosMessage.PrepareReq.parseFrom(request.getBody());
            Message.PrepareRequest req = new Message.PrepareRequest(request.getSender(), request.getInstanceId(), prepareReq.getBallot());

            Message.PrepareResponse result = ProtoRequestDispatcher.this.acceptor.prepare(req);

            PaxosMessage.PrepareRes.Builder builder = PaxosMessage.PrepareRes.newBuilder();
            builder.setSuccess(result.maxBallot() == req.ballot());
            builder.setMaxBallot(result.maxBallot());
            builder.setAcceptedBallot(result.acceptedBallot());
            builder.setAcceptedValue(ByteString.copyFrom(result.acceptedValue()));

            return Pair.of(PaxosMessage.Code.PREPARE_RES, builder.build().toByteString());
        }
    }
}
