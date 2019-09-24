package org.axesoft.tans.server;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.axesoft.jaxos.algo.Proponent;
import org.axesoft.jaxos.algo.ProposeResult;
import org.axesoft.jaxos.algo.StateMachine;
import org.axesoft.tans.protobuff.TansMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        this.numberMaps = new TansNumberMap[this.config.jaxConfig().partitionNumber()];
        this.machineLocks = new Object[this.config.jaxConfig().partitionNumber()];
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

    public List<LongRange> acquire(int squadId, List<KeyLong> requests) throws InterruptedException {
        checkArgument(requests.size() > 0, "requests is empty");

        TansNumberProposal proposal;
        ProposeResult result;

        synchronized (machineLocks[squadId]) {
            proposal = this.numberMaps[squadId].createProposal(requests);
            ByteString bx = toMessage(proposal.numbers);
            result = proponent.get().propose(squadId, proposal.instanceId, bx);
        }

        if (!result.isSuccess()) {
            logger.debug("Try acquire on key({}) abort by {}", requests, result.code());
        }

        if (result.code() == ProposeResult.Code.OTHER_LEADER) {
            throw new RedirectException((int) result.param());
        }

        if (result.isSuccess()) {
            return produceResult(requests, proposal.numbers);
        }
        else if (result.code() == ProposeResult.Code.CONFLICT) {
            throw new ConflictException();
        }
        else {
            throw new IllegalStateException(result.code().toString());
        }
    }

    private List<LongRange> produceResult(List<KeyLong> requests, List<TansNumber> numbers) {
        ImmutableList.Builder<LongRange> builder = ImmutableList.builder();
        for (int i = 0; i < requests.size(); i++) {
            KeyLong req = requests.get(i);
            TansNumber n = numbers.get(i);

            builder.add(new LongRange(n.value() - req.value(), n.value() - 1));
        }
        return builder.build();
    }

    public int squadIdOf(String key) {
        int code = key.hashCode();
        if (code == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(code) % this.numberMaps.length;
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
            for (TansNumber n1 : nx) {
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

        synchronized TansNumberProposal createProposal(List<KeyLong> requests) {
            ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
            Map<String, TansNumber> newNumberMap = new HashMap<>();
            for (KeyLong k : requests) {
                TansNumber n1, n0 = newNumberMap.get(k.key());
                if (n0 == null) {
                    n0 = this.numbers.get(k.key());
                }
                if (n0 == null) {
                    n1 = new TansNumber(k.key(), k.value() + 1);
                }
                else {
                    n1 = n0.update(k.value());
                }

                builder.add(n1);
                newNumberMap.put(k.key(), n1);
            }
            return new TansNumberProposal(this.lastInstanceId + 1, builder.build());
        }
    }

    private static class TansNumberProposal {
        final long instanceId;
        final List<TansNumber> numbers;

        public TansNumberProposal(long instanceId, List<TansNumber> numbers) {
            this.instanceId = instanceId;
            this.numbers = numbers;
        }

        @Override
        public String toString() {
            return "TansNumberProposal{" +
                    ", instanceId=" + instanceId +
                    ", numbers=" + numbers +
                    '}';
        }
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
}
