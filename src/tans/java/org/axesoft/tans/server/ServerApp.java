package org.axesoft.tans.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import org.axesoft.jaxos.JaxosService;
import org.axesoft.jaxos.algo.Proponent;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {
    private TansConfig config;
    private JaxosService jaxosService;
    private HttpApiService httpApiService;
    private TansService tansService;
    private ServiceManager manager;
    private Timer printMetricsTimer;

    private ServerApp(TansConfig config) {
        this.config = config;
        this.tansService = new TansService(config, () -> this.jaxosService);
        this.jaxosService = new JaxosService(config.jaxConfig(), tansService);
        this.httpApiService = new HttpApiService(config, tansService);
        this.manager = new ServiceManager(ImmutableList.of(this.jaxosService, this.httpApiService));
    }

    private void start(){
        this.printMetricsTimer = new Timer("Metrics Thread", true);
        this.printMetricsTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                httpApiService.printMetrics();
                jaxosService.printMetrics();
            }
        }, 3000, config.printMetricsIntervalSeconds() * 1000);

        manager.startAsync();
    }

    public void shutdown(){
        printMetricsTimer.cancel();
        manager.stopAsync();
        try {
            manager.awaitStopped(10, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            e.printStackTrace(System.err);
        }
    }

    public static void main(String[] args) throws Exception {
        TansConfig config = new ArgumentParser().parse(args);
        //prepare it for logback
        System.setProperty("node-id", Integer.toString(config.serverId()));

        ServerApp app = new ServerApp(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> app.shutdown()));

        app.start();
    }
}
