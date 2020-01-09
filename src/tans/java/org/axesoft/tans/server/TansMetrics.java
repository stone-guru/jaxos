package org.axesoft.tans.server;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author bison
 * @sine 2019/12/26.
 */
public class TansMetrics {
    private  PrometheusMeterRegistry registry;
    private Counter requestCounter;
    private Counter redirectCounter;
    private Timer requestTimer;

    public TansMetrics(int serverId, int squadCount, Function<Integer, Number> keyCountFunction) {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        this.registry.config().commonTags("server", Integer.toString(serverId));
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);

        this.requestCounter = Counter.builder("tans.request.total")
                .description("The total times of TANS request")
                .register(registry);

        this.redirectCounter = Counter.builder("tans.request.redirect")
                .description("The success times of propose request")
                .register(registry);

        this.requestTimer = Timer.builder("tans.request.elapsed")
                .description("The time for each request")
                .publishPercentiles(0.10, 0.20, 0.5, 0.80, 0.90)
                .sla(Duration.ofMillis(3))
                .minimumExpectedValue(Duration.ofNanos(200_000))
                .register(registry);

        for(int i = 0; i < squadCount; i++) {
            final int squadId = i;
            Gauge.builder("tans.key.count", () -> keyCountFunction.apply(squadId))
                    .description("The count of keys in squad " + squadId)
                    .tag("squad", Integer.toString(squadId))
                    .register(registry);
        }
    }

    public void incRequestCount() {
        this.requestCounter.increment();
    }

    public void incRedirectCounter() {
        this.redirectCounter.increment();
    }

    public void recordRequestElapsed(long millis) {
        requestTimer.record(Long.max(millis, 1), TimeUnit.MILLISECONDS);
    }


    public String format() {
        return registry.scrape();
    }
}
