/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.jaxos.app;

import org.jaxos.JaxosConfig;
import org.jaxos.algo.Event;
import org.jaxos.netty.NettySenderFactory;
import org.jaxos.network.RequestSender;

public class ClientApp {

    public static void main(String[] args) throws Exception{
        JaxosConfig config = JaxosConfig.builder()
                .setServerId(1)
                .setPort(9999)
                .addPeer(0, "192.168.1.164", 9999)
                .build();

        NettySenderFactory factory = new NettySenderFactory();
        RequestSender sender = factory.createSender(config);

        for(int i = 0; i < 10 ; i++) {
            int ballot = 10*(i + 1);
            Event.PrepareRequest req = new Event.PrepareRequest(config.serverId(), 1000, ballot);
            sender.broadcast(req);
            Event.AcceptRequest accept = new Event.AcceptRequest(config.serverId(), 1000, ballot, new byte[]{});
            sender.broadcast(accept);

            Thread.sleep(1000);
        }
    }
}
