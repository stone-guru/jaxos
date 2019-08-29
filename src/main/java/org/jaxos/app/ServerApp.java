package org.jaxos.app;

import org.jaxos.JaxosConfig;
import org.jaxos.netty.NettyServer;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {
    public static void main(String[] args) {
        JaxosConfig config = new ArgumentParser().parse(args);
        NettyServer server = new NettyServer(config);
        server.startup();
    }
}
