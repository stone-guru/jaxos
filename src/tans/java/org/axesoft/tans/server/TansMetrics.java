package org.axesoft.tans.server;

import io.micrometer.core.instrument.Counter;
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

/**
 * @author bison
 * @sine 2019/12/26.
 */
public class TansMetrics {
    private static PrometheusMeterRegistry registry;

    public static TansMetrics buildInstance(int serverId) { //FIXME not good smell for passing serverId like this
        if (registry == null) {
            synchronized (TansMetrics.class) {
                if (registry == null) {
                    registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
                    registry.config().commonTags("server", Integer.toString(serverId));
                    new ClassLoaderMetrics().bindTo(registry);
                    new JvmMemoryMetrics().bindTo(registry);
                    new JvmGcMetrics().bindTo(registry);
                    new ProcessorMetrics().bindTo(registry);
                    new JvmThreadMetrics().bindTo(registry);
                }
            }
        }
        return new TansMetrics();
    }

    public static String scrape() {
        return registry.scrape();
    }

    private Counter requestCounter;
    private Counter redirectCounter;
    private Timer requestTimer;

    public TansMetrics() {
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
}
