package org.axesoft.tans.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import org.axesoft.jaxos.JaxosService;

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
    private ServiceManager serviceManager;
    private PartedThreadPool requestThreadPool;

    private ServerApp(TansConfig config) {
        this.config = config;
        this.tansService = new TansService(config, () -> this.jaxosService);
        this.requestThreadPool = new PartedThreadPool(this.config.jaxConfig().partitionNumber(), "Request Process Thread");
        this.jaxosService = new JaxosService(config.jaxConfig(), tansService, this.requestThreadPool);
        this.httpApiService = new HttpApiService(config, tansService, this.requestThreadPool);
        this.serviceManager = new ServiceManager(ImmutableList.of(this.jaxosService, this.httpApiService));
    }

    private void start() {
        serviceManager.startAsync();
    }

    public void shutdown() {
        serviceManager.stopAsync();
        try {
            serviceManager.awaitStopped(10, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
            e.printStackTrace(System.err);
        }
    }

    public static void main(String[] args) throws Exception {
        TansConfig config = new ArgumentParser().parse(args);
        //set for logback to store log in different files
        System.setProperty("node-id", Integer.toString(config.serverId()));

        ServerApp app = new ServerApp(config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> app.shutdown()));

        app.start();
    }
}
