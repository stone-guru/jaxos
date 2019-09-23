package org.axesoft.tans.server;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.algo.Proponent;
import org.axesoft.jaxos.algo.ProposeResult;
import org.axesoft.jaxos.algo.StateMachine;
import org.axesoft.jaxos.base.Either;
import org.axesoft.tans.protobuff.TansMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
        for (int i = 0; i < numberMaps.length; i++) {
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
        List<TansNumber> nx = fromMessage(message);
        if (logger.isTraceEnabled()) {
            logger.trace("TANS state machine consumer {} event from instance {}", nx.size(), instanceId);
        }
        this.numberMaps[squadId].consume(instanceId, nx);
    }

    @Override
    public void close() {
        logger.info("TANS state machine closed");
    }

    public List<Pair<Long, Long>> acquire(int squadId, List<Pair<String, Long>> requests){
        return Collections.emptyList();
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

        int squadId = squadIdOf(k);

        int i = 0;
        ProposeResult result;
        TansNumber n;

        synchronized (machineLocks[squadId]) {
            do {
                TansNumberProposal p = createProposal(squadId, ImmutableList.of(Pair.of(k, v)));
                n = p.numbers.get(0); //FIXME
                logger.trace("TansService prepare proposal {}", p);
                ByteString bx = toMessage(p.numbers);
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


    private TansNumberProposal createProposal(int squadId, List<Pair<String, Long>> requests) {
        Pair<Long, List<TansNumber>> p = this.numberMaps[squadId].createProposal(requests);
        return new TansNumberProposal(squadId, p.getLeft(), p.getRight());
    }

    public int squadIdOf(String key) {
        int code = key.hashCode();
        if (code == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(code) % this.numberMaps.length;
    }


    private static ByteString toMessage(List<TansNumber> nx) {
        TansMessage.TansProposal.Builder builder = TansMessage.TansProposal.newBuilder();
        for (TansNumber n : nx) {
            TansMessage.ProtoTansNumber.Builder nb = TansMessage.ProtoTansNumber.newBuilder()
                    .setName(n.name())
                    .setValue(n.value())
                    .setVersion(n.version())
                    .setTimestamp(n.timestamp())
                    .setVersion0(n.version0())
                    .setValue0(n.value0());

            builder.addNumber(nb);
        }

        return builder.build().toByteString();
    }

    private static List<TansNumber> fromMessage(ByteString message) {
        TansMessage.TansProposal p;
        try {
            p = TansMessage.TansProposal.parseFrom(message);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        if (p.getNumberCount() == 0) {
            throw new RuntimeException("Empty tans number list");
        }

        ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
        for (TansMessage.ProtoTansNumber number : p.getNumberList()) {
            builder.add(new TansNumber(number.getName(), number.getVersion(), number.getTimestamp(), number.getValue(),
                    number.getVersion0(), number.getValue0()));
        }

        return builder.build();
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

        synchronized void consume(long instanceId, List<TansNumber> nx) {
            if (instanceId != this.lastInstanceId + 1) {
                throw new IllegalStateException(String.format("dolog %d when current is %d", instanceId, this.lastInstanceId));
            }
            for(TansNumber n1 : nx) {
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
            this.lastInstanceId = instanceId;
        }

        synchronized Pair<Long, TansNumber> createProposal(String name, long v) {
            Pair<Long, List<TansNumber>> result = createProposal(Collections.singletonList(Pair.of(name, v)));
            return Pair.of(result.getLeft(), result.getRight().get(0));
        }

        synchronized Pair<Long, List<TansNumber>> createProposal(List<Pair<String, Long>> requests){
            ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
            Map<String, TansNumber>  newNumberMap = new HashMap<>();
            for(Pair<String, Long> p : requests){
                TansNumber n1, n0 = newNumberMap.get(p.getKey());
                if(n0 == null){
                    n0 = this.numbers.get(p.getKey());
                }
                if(n0 == null){
                    n1 = new TansNumber(p.getKey(), p.getValue() + 1);
                } else {
                    n1 = n0.update(p.getValue());
                }

                builder.add(n1);
                newNumberMap.put(p.getKey(), n1);
            }
            return Pair.of(this.lastInstanceId + 1, builder.build());
        }
    }

    private static class TansNumberProposal {
        final int squadId;
        final long instanceId;
        final List<TansNumber> numbers;

        public TansNumberProposal(int squadId, long instanceId, List<TansNumber> numbers) {
            this.squadId = squadId;
            this.instanceId = instanceId;
            this.numbers = numbers;
        }

        @Override
        public String toString() {
            return "TansNumberProposal{" +
                    "squadId=" + squadId +
                    ", instanceId=" + instanceId +
                    ", numbers=" + numbers +
                    '}';
        }
    }
}
