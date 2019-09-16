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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TansService implements StateMachine {
    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    private final Map<String, TansNumber> numbers = new HashMap<>();
    private Supplier<Proponent> proponent;
    private TansConfig config;
    private volatile  long lastInstanceId;

    public TansService(TansConfig config, Supplier<Proponent> proponent) {
        this.proponent = checkNotNull(proponent);
        this.config = config;
    }

    @Override
    public void learnLastChosenVersion(long instanceId) {
        this.lastInstanceId = instanceId;
    }

    @Override
    public long currentVersion() {
        synchronized (numbers){
            return this.lastInstanceId;
        }
    }

    @Override
    public void consume(long instanceId, ByteString message) {
        TansNumber n1 = fromMessage(message);

        logger.trace("TANS state machine consumer event {}", n1);
        synchronized (numbers) {
            if(instanceId != this.lastInstanceId + 1){
                throw new IllegalStateException(String.format("dolog %d when current is %d", instanceId, this.lastInstanceId));
            }
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
            this.lastInstanceId = instanceId;
        }
    }


    /**
     * @param k
     * @param v
     * @return
     * @throws IllegalStateException
     * @throws IllegalArgumentException
     * @throws RedirectException
     */
    public Pair<Long, Long> acquire(String k, long v) {
        checkNotNull(k);
        checkArgument(v > 0, "required number {} is negative");
        logger.trace("TanService acquire {} for {}", k, v);

        int i = 0;
        ProposeResult result;
        TansNumber n;
        do {
            Pair<Long, TansNumber> p = createProposal(k, v);
            n = p.getRight();
            logger.trace("TansService prepare proposal {}", p);
            ByteString bx = toMessage(p.getRight());
            try {
                result = proponent.get().propose(p.getLeft(), bx);
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
        } while (i < 2 && !result.isSuccess());

        if (result.isSuccess()) {
            return Pair.of(n.value() - v, n.value() - 1);
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

    private Pair<Long, TansNumber> createProposal(String name, long v) {
        synchronized (numbers) {
            TansNumber n0 = numbers.get(name);
            if (n0 == null) {
                return Pair.of(this.lastInstanceId + 1, new TansNumber(name, v + 1));
            }
            else {
                return Pair.of(this.lastInstanceId + 1, n0.update(v));
            }
        }
    }

    private ByteString toMessage(TansNumber n) {
        return TansMessage.ProtoTansNumber.newBuilder()
                .setName(n.name())
                .setValue(n.value())
                .setVersion(n.version())
                .setTimestamp(n.timestamp())
                .setVersion0(n.version0())
                .setValue0(n.value0())
                .build()
                .toByteString();
    }

    private TansNumber fromMessage(ByteString message) {
        try {
            TansMessage.ProtoTansNumber number = TansMessage.ProtoTansNumber.parseFrom(message);
            return new TansNumber(number.getName(), number.getVersion(), number.getTimestamp(), number.getValue(), number.getVersion0(), number.getValue0());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
