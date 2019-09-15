package org.axesoft.tans.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import org.axesoft.jaxos.JaxosService;
import org.axesoft.jaxos.algo.Proponent;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {
    private static Proponent proponent;

    public static void main(String[] args) throws Exception {
        TansConfig config = new ArgumentParser().parse(args);
        //prepare it for logback
        System.setProperty("node-id", Integer.toString(config.serverId()));

        TansService tansService = new TansService(config, () -> proponent);

        final JaxosService jaxosService = new JaxosService(config.jaxConfig(), tansService);
        ServerApp.proponent = jaxosService.getProponent();

        final HttpApiService httpApiService = new HttpApiService(config, tansService);

        ServiceManager manager = new ServiceManager(ImmutableList.of(jaxosService, httpApiService));

        manager.startAsync();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> manager.stopAsync()));
    }
}
