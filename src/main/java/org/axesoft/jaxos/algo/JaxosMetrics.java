package org.axesoft.jaxos.algo;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.base.Velometer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class JaxosMetrics {
    private static PrometheusMeterRegistry registry;

    public enum ProposalResult {
        SUCCESS, CONFLICT, OTHER
    }

    private Velometer proposeVelometer = new Velometer();
    private Velometer acceptVelometer = new Velometer();
    private Velometer conflictVelometer = new Velometer();
    private Velometer successVelometer = new Velometer();
    private Velometer otherVelometer = new Velometer();

    private final int squadId;
    private final int serverId;

    private Counter proposeCounter;
    private Counter successCounter;
    private Counter conflictCounter;
    private Timer proposeTimer;

    public JaxosMetrics(int serverId, int squadId) {
        this.serverId = serverId;
        this.squadId = squadId;
        if (registry == null) {
            registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            registry.config().commonTags("server", Integer.toString(this.serverId));
        }

        this.proposeCounter = Counter.builder("propose.total")
                .description("The total times of propose request")
                .tags("squad", Integer.toString(this.squadId))
                .register(registry);

        this.successCounter = Counter.builder("propose.success")
                .description("The success times of propose request")
                .tags("squad", Integer.toString(this.squadId))
                .register(registry);

        this.conflictCounter = Counter.builder("propose.conflict")
                .description("The conflict times of propose request")
                .tags("squad", Integer.toString(this.squadId))
                .register(registry);

        this.proposeTimer = Timer.builder("propose.elapsed")
                .description("The time for each propose")
                .tags("squad", Integer.toString(this.squadId))
                .publishPercentiles(0.10, 0.20, 0.5, 0.80, 0.90)
                .sla(Duration.ofMillis(3))
                .minimumExpectedValue(Duration.ofNanos(200_000))
                .register(registry);
    }

    public void recordAccept(long nanos) {
        acceptVelometer.record(nanos);
    }

    public void recordPropose(long nanos, ProposalResult result) {
        proposeCounter.increment();
        proposeTimer.record(nanos, TimeUnit.NANOSECONDS);

        proposeVelometer.record(nanos);
        switch (result) {
            case SUCCESS:
                successVelometer.record(1);
                successCounter.increment();
                break;
            case CONFLICT:
                conflictVelometer.record(1);
                conflictCounter.increment();
                break;
            default:
                otherVelometer.record(1);
        }
    }

    public long lastTime() {
        return proposeVelometer.lastTimestamp();
    }

    public long proposeTimes() {
        return proposeVelometer.times();
    }

    public long proposeDelta() {
        return proposeVelometer.timesDelta();
    }

    public long acceptTimes() {
        return acceptVelometer.times();
    }

    public long acceptDelta() {
        return acceptVelometer.timesDelta();
    }

    public Pair<Double, Double> compute(long timestamp) {
        double currentProposeElapsed = proposeVelometer.compute(timestamp);
        double currentAcceptElapsed = acceptVelometer.compute(timestamp);
        this.conflictVelometer.compute(timestamp);
        this.successVelometer.compute(timestamp);
        this.otherVelometer.compute(timestamp);

        return Pair.of(currentProposeElapsed, currentAcceptElapsed);
    }

    public double totalSuccessRate() {
        return this.successVelometer.times() / (double) proposeVelometer.times();
    }

    public double successRate() {
        return successVelometer.timesDelta() / (double) proposeVelometer.timesDelta();
    }

    public double conflictRate() {
        return conflictVelometer.timesDelta() / (double) proposeVelometer.timesDelta();
    }

    public double otherRate() {
        return otherVelometer.timesDelta() / (double) proposeVelometer.timesDelta();
    }

    public static String scrape() {
        return registry.scrape();
    }
}
