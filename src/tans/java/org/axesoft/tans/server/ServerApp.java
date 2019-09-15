package org.axesoft.tans.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ServiceManager;
import org.axesoft.jaxos.JaxosService;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {

    public static void main(String[] args) throws Exception {
        TansConfig config = new ArgumentParser().parse(args);
        //prepare it for logback
        System.setProperty("node-id", Integer.toString(config.serverId()));

        final JaxosService jaxosService = new JaxosService(config.jaxConfig(), null);
        final HttpApiService httpApiService = new HttpApiService(jaxosService.getProponent(), config);

        ServiceManager manager = new ServiceManager(ImmutableList.of(jaxosService, httpApiService));

        manager.startAsync();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> manager.stopAsync()));
    }
}
