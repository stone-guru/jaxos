package org.jaxos.app;

import org.jaxos.JaxosConfig;
import org.jaxos.httpserver.HttpApiServer;
import org.jaxos.netty.NettyJaxosNode;

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
