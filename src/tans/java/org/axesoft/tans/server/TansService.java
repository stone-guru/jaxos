package org.axesoft.tans.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.axesoft.jaxos.algo.Proponent;
import org.axesoft.jaxos.algo.ProposeResult;
import org.axesoft.jaxos.algo.StateMachine;
import org.axesoft.tans.protobuff.TansMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TansService implements StateMachine {
    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    private ConcurrentHashMap<String, TansNumber> numbers = new ConcurrentHashMap<>();
    private Supplier<Proponent> proponent;
    private TansConfig config;

    public TansService(TansConfig config, Supplier<Proponent> proponent) {
        this.proponent = checkNotNull(proponent);
        this.config = config;
    }

    @Override
    public void consume(long instanceId, ByteString message) {
        TansNumber n1 = fromMessage(message);

        logger.trace("TANS state machine consumer event {}", n1);
        numbers.compute(n1.name(), (k, n0) -> {
            if (n0 == null) {
                return n1;
            }
            else if (n1.version() == n0.version() + 1) {
                return n1;
            }
            else {
                throw new RuntimeException(String.format("Unmatched version n0 = %s, n1 = %s", n0, n1));
            }
        });
    }


    /**
     * @param k
     * @param v
     * @return
     * @throws IllegalStateException
     * @throws IllegalArgumentException
     * @throws RedirectException
     */
    public synchronized Pair<Long, Long> acquire(String k, long v) {
        checkNotNull(k);
        checkArgument(v > 0, "required number {} is negative");
        logger.trace("TanService acquire {} for {}", k, v);
        ;

        int i = 0;
        ProposeResult result;
        TansNumber n;
        do {
            n = createProposal(k, v);
            ByteString bx = toMessage(n);
            try {
                result = proponent.get().propose(bx);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            i++;

            if (!result.isSuccess()) {
                logger.warn("Request on key({}) failed by {}", k, result.code());
            }

            if (result.code() == ProposeResult.Code.OTHER_LEADER) {
                throw new RedirectException(getTargetUrl((int) result.param()));
            }
        } while (i < 3 && !result.isSuccess());

        if (result.isSuccess()) {
            return Pair.of(n.value() - v , n.value() - 1);
        }
        else if (result.code() == ProposeResult.Code.CONFLICT) {
            throw new ConflictException();
        }
        else {
            throw new IllegalStateException(result.code().toString());
        }
    }

    private String getTargetUrl(int serverId) {
        int httpPort = config.getPeerHttpPort(serverId);
        JaxosSettings.Peer peer = config.jaxConfig().getPeer(serverId);
        return String.format("http://%s:%s", peer.address(), httpPort);
    }

    private synchronized TansNumber createProposal(String name, long v) {
        TansNumber n0 = numbers.get(name), n1;
        if (n0 == null) {
            return new TansNumber(name, v + 1);
        }
        else {
            return n0.update(v);
        }
    }

    private ByteString toMessage(TansNumber n) {
        return TansMessage.ProtoTansNumber.newBuilder()
                .setName(n.name())
                .setValue(n.value())
                .setVersion(n.version())
                .setTimestamp(n.timestamp())
                .build()
                .toByteString();
    }

    private TansNumber fromMessage(ByteString message) {
        try {
            TansMessage.ProtoTansNumber number = TansMessage.ProtoTansNumber.parseFrom(message);
            return new TansNumber(number.getName(), number.getVersion(), number.getTimestamp(), number.getValue());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
