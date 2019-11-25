package org.axesoft.tans.server;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.base.LongRange;
import org.axesoft.tans.protobuff.TansMessage;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TansService implements StateMachine {
    private static Logger logger = LoggerFactory.getLogger(TansService.class);

    public static class AcquireRequest {

    }
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
    public void learnLastChosenVersion(int squadId, long instanceId) {
        this.numberMaps[squadId].learnLastChosenVersion(instanceId);
    }

    @Override
    public long currentVersion(int squadId) {
        return this.numberMaps[squadId].currentVersion();
    }

    @Override
    public void consume(int squadId, long instanceId, ByteString proposal) {
        List<TansNumber> nx = proposal.isEmpty()? Collections.emptyList() : fromProposal(proposal);
        if (logger.isTraceEnabled()) {
            logger.trace("TANS state machine consumer {} event from instance {}.{}", nx.size(), squadId, instanceId);
        }
        this.numberMaps[squadId].consume(instanceId, nx);
    }

    @Override
    public CheckPoint makeCheckPoint(int squadId) {
        Pair<Collection<TansNumber>, Long> p = this.numberMaps[squadId].getSnapshot();
        long timestamp = System.currentTimeMillis();
        ByteString content = toCheckPoint(p.getLeft());
        return new CheckPoint(squadId, p.getRight(), timestamp, content);
    }

    @Override
    public void restoreFromCheckPoint(CheckPoint checkPoint) {
        List<TansNumber> nx = fromCheckPoint(checkPoint.content());
        this.numberMaps[checkPoint.squadId()].transToCheckPoint(checkPoint.instanceId(), nx);
    }

    @Override
    public void close() {
        logger.info("TANS state machine closed");
    }

    public List<LongRange> acquire(int squadId, List<KeyLong> requests, boolean ignoreLeader) {
        checkArgument(requests.size() > 0, "requests is empty");

        TansNumberProposal proposal;
        ListenableFuture<Void> resulFuture;

        synchronized (machineLocks[squadId]) {
            proposal = this.numberMaps[squadId].createProposal(requests);
            ByteString bx = toProposal(proposal.numbers);
            resulFuture = proponent.get().propose(squadId, proposal.instanceId, bx, ignoreLeader);
        }

        try {
            resulFuture.get();

            return produceResult(requests, proposal.numbers);
        }
        catch (InterruptedException e) {
            //logger.debug("Execution interrupted", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            logger.debug("Try acquire on key({}) abort by {}", requests, e.getCause().getMessage());

            if (e.getCause() instanceof RedirectException) {
                throw (RedirectException) e.getCause();
            }
            else if (e.getCause() instanceof ProposalConflictException) {
                throw (ProposalConflictException) e.getCause();
            }
            else {
                throw new IllegalArgumentException(e.getCause());
            }
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
        private PMap<String, TansNumber> numbers = HashTreePMap.empty();
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

            this.numbers = applyChange(nx, this.numbers);
            this.lastInstanceId = instanceId;
        }

        private PMap<String, TansNumber> applyChange(List<TansNumber> nx, PMap<String, TansNumber> numbers0) {
            PMap<String, TansNumber> numbers1 = numbers0;
            for (TansNumber n1 : nx) {
                TansNumber n0 = numbers1.get(n1.name());
                if (n0 == null) {
                    numbers1 = numbers1.plus(n1.name(), n1);
                }
                else {
                    if (n1.version() == n0.version() + 1) {
                        numbers1 = numbers1.plus(n1.name(), n1);
                    }
                    else {
                        throw new RuntimeException(String.format("Unmatched version n0 = %s, n1 = %s", n0, n1));
                    }
                }
            }
            return numbers1;
        }

        synchronized TansNumberProposal createProposal(List<KeyLong> requests) {
            ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
            PMap<String, TansNumber> numbers1 = this.numbers;
            for (KeyLong k : requests) {
                TansNumber n1, n0;
                n0 = numbers1.get(k.key());
                if (n0 == null) {
                    n1 = new TansNumber(k.key(), k.value() + 1);
                }
                else {
                    n1 = n0.update(k.value());
                }

                builder.add(n1);
                numbers1 = numbers1.plus(k.key(), n1);
            }
            return new TansNumberProposal(this.lastInstanceId + 1, builder.build());
        }

        synchronized Pair<Collection<TansNumber>, Long> getSnapshot() {
            return Pair.of(this.numbers.values(), lastInstanceId);
        }

        synchronized void transToCheckPoint(long instanceId, List<TansNumber> nx) {
            this.lastInstanceId = instanceId;
            Map<String, TansNumber> m = nx.stream().collect(Collectors.toMap(TansNumber::name, Functions.identity()));
            this.numbers = HashTreePMap.from(m);
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


    private static ByteString toProposal(List<TansNumber> nx) {
        TansMessage.TansProposal.Builder builder = TansMessage.TansProposal.newBuilder();
        for (TansNumber n : nx) {
            TansMessage.ProtoTansNumber.Builder nb = TansMessage.ProtoTansNumber.newBuilder()
                    .setName(n.name())
                    .setValue(n.value())
                    .setVersion(n.version())
                    .setTimestamp(n.timestamp());

            TansMessage.NumberProposal.Builder pb = TansMessage.NumberProposal.newBuilder()
                    .setNumber(nb)
                    .setVersion0(n.version0())
                    .setValue0(n.value0());

            builder.addProposal(pb);
        }

        return builder.build().toByteString();
    }

    private static List<TansNumber> fromProposal(ByteString message) {
        TansMessage.TansProposal proposal;
        try {
            proposal = TansMessage.TansProposal.parseFrom(message);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        if (proposal.getProposalCount() == 0) {
            throw new RuntimeException("Empty tans number list");
        }

        ImmutableList.Builder<TansNumber> builder = ImmutableList.builder();
        for (TansMessage.NumberProposal np : proposal.getProposalList()) {
            TansMessage.ProtoTansNumber n = np.getNumber();
            builder.add(new TansNumber(n.getName(), n.getVersion(), n.getTimestamp(), n.getValue(),
                    np.getVersion0(), np.getValue0()));
        }

        return builder.build();
    }


    private static ByteString toCheckPoint(Collection<TansNumber> nx) {
        TansMessage.TansCheckPoint.Builder cb = TansMessage.TansCheckPoint.newBuilder();

        for (TansNumber n : nx) {
            TansMessage.ProtoTansNumber.Builder nb = TansMessage.ProtoTansNumber.newBuilder()
                    .setName(n.name())
                    .setValue(n.value())
                    .setVersion(n.version())
                    .setTimestamp(n.timestamp());

            cb.addNumber(nb);
        }

        return cb.build().toByteString();
    }

    private static List<TansNumber> fromCheckPoint(ByteString content) {
        TansMessage.TansCheckPoint checkPoint;
        try {
            checkPoint = TansMessage.TansCheckPoint.parseFrom(content);
        }
        catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        return checkPoint.getNumberList().stream()
                .map(n -> new TansNumber(n.getName(), n.getVersion(), n.getTimestamp(), n.getValue()))
                .collect(Collectors.toList());
    }
}
