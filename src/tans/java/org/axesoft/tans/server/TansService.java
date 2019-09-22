package org.axesoft.tans.server;

import com.google.common.base.Strings;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TansService implements StateMachine {
    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    private Supplier<Proponent> proponent;
    private TansConfig config;

    private TansNumberMap[] numberMaps;
    private Object[] machineLocks;

    public TansService(TansConfig config, Supplier<Proponent> proponent) {
        this.proponent = checkNotNull(proponent);
        this.config = config;

        this.numberMaps = new TansNumberMap[config.jaxConfig().partitionNumber()];
        this.machineLocks = new Object[config.jaxConfig().partitionNumber()];
        for(int i = 0; i < numberMaps.length; i++){
            this.numberMaps[i] = new TansNumberMap();
            this.machineLocks[i] = new Object();
        }
    }

    @Override
    public void learnLastChosenVersion(int squaldId, long instanceId) {
        this.numberMaps[squaldId].learnLastChosenVersion(instanceId);
    }

    @Override
    public long currentVersion(int squadId) {
        return this.numberMaps[squadId].currentVersion();
    }

    @Override
    public void consume(int squadId, long instanceId, ByteString message) {
        TansNumber n1 = fromMessage(message);
        if (logger.isTraceEnabled()) {
            logger.trace("TANS state machine consumer event {}", n1);
        }
        this.numberMaps[squadId].consume(instanceId, n1);
    }

    @Override
    public void close() {
        logger.info("TANS state machine closed");
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
        checkArgument(!Strings.isNullOrEmpty(k), "key name is null or empty");
        checkArgument(v > 0, "required number {} is negative");
        logger.trace("TanService acquire {} for {}", k, v);

        int squadId = selectSquadId(k);

        int i = 0;
        ProposeResult result;
        TansNumber n;

        synchronized (machineLocks[squadId]) {
            do {
                TansNumberProposal p = createProposal(squadId, k, v);
                n = p.number;
                logger.trace("TansService prepare proposal {}", p);
                ByteString bx = toMessage(p.number);
                try {
                    result = proponent.get().propose(p.squadId, p.instanceId, bx);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                i++;

                if (!result.isSuccess()) {
                    logger.debug("Try acquire on key({}) abort by {}", k, result.code());
                }

                if (result.code() == ProposeResult.Code.OTHER_LEADER) {
                    throw new RedirectException((int) result.param());
                }
            } while (i < 2 && !result.isSuccess());
        }

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


    private TansNumberProposal createProposal(int squadId, String name, long v) {
        Pair<Long, TansNumber> p = this.numberMaps[squadId].createProposal(name, v);
        return new TansNumberProposal(squadId, p.getLeft(), p.getRight());
    }

    private int selectSquadId(String key){
        int code = key.hashCode();
        if(code == Integer.MIN_VALUE){
            return 0;
        }
        return Math.abs(code) % this.numberMaps.length;
    }

    private static ByteString toMessage(TansNumber n) {
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

    private static TansNumber fromMessage(ByteString message) {
        try {
            TansMessage.ProtoTansNumber number = TansMessage.ProtoTansNumber.parseFrom(message);
            return new TansNumber(number.getName(), number.getVersion(), number.getTimestamp(), number.getValue(), number.getVersion0(), number.getValue0());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }


    private static class TansNumberMap {
        private final Map<String, TansNumber> numbers = new HashMap<>();
        private long lastInstanceId;

        synchronized long currentVersion() {
            return this.lastInstanceId;
        }

        synchronized void learnLastChosenVersion(long instanceId) {
            this.lastInstanceId = instanceId;
        }

        synchronized void consume(long instanceId, TansNumber n1) {
            if (instanceId != this.lastInstanceId + 1) {
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

        synchronized Pair<Long, TansNumber> createProposal(String name, long v) {
            TansNumber n0 = numbers.get(name);
            if (n0 == null) {
                return Pair.of(this.lastInstanceId + 1, new TansNumber(name, v + 1));
            }
            else {
                return Pair.of(this.lastInstanceId + 1, n0.update(v));
            }
        }
    }

    private static class TansNumberProposal {
        final int squadId;
        final long instanceId;
        final TansNumber number;

        public TansNumberProposal(int squadId, long instanceId, TansNumber number) {
            this.squadId = squadId;
            this.instanceId = instanceId;
            this.number = number;
        }

        @Override
        public String toString() {
            return "TansNumberProposal{" +
                    "squadId=" + squadId +
                    ", instanceId=" + instanceId +
                    ", number=" + number +
                    '}';
        }
    }
}
