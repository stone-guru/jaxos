package org.jaxos.app;

import org.jaxos.JaxosConfig;
import org.jaxos.netty.NettyServer;

/**
 * @author gaoyuan
 * @sine 2019/8/25.
 */
public class ServerApp {
    public static void main(String[] args) {
        JaxosConfig config = JaxosConfig.builder()
                .setServerId(0)
                .setPort(9999)
                .addPeer(1, "localhost", 9999)
                .build();

        NettyServer server = new NettyServer(config);
        server.startup();
    }
}
