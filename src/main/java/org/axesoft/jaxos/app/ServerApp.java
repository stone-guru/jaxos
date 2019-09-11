package org.axesoft.jaxos.app;

import org.axesoft.jaxos.JaxosConfig;
import org.axesoft.jaxos.httpserver.HttpApiServer;
import org.axesoft.jaxos.netty.NettyJaxosNode;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {
    public static void main(String[] args) throws Exception {
        JaxosConfig config = new ArgumentParser().parse(args);
        NettyJaxosNode server = new NettyJaxosNode(config);
        new Thread(null, () -> server.startup(), "JaxosNodeThread").start();

        HttpApiServer httpServer = new HttpApiServer(server.instance(), config.self().address(), config.self().httpPort());
        httpServer.start();
    }
}
