package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.JaxosConfig;
import org.junit.Test;

public class ProposerTest {

    private Proposer createProposer() {
        JaxosConfig config = JaxosConfig.builder()
                .setServerId(1)
                .setSelf(new JaxosConfig.Peer(1, "localhost", 9991, 8081))
                .addPeer(new JaxosConfig.Peer(2, "localhost", 9992, 8082))
                .addPeer(new JaxosConfig.Peer(3, "localhost", 9993, 8083))
                .build();

        Communicator communicator = new Communicator() {
            @Override
            public boolean available() {
                return true;
            }

            @Override
            public void broadcast(Event msg) {
                System.out.println(msg);
            }

            @Override
            public void close() {

            }
        };

        return new Proposer(config, new InstanceContext(1, config), () -> communicator);
    }

    @Test
    public void testAccept1() throws Exception {
        Proposer p = createProposer();
        p.propose(ByteString.copyFromUtf8("Good good study"));
        Thread.sleep(2);

        new Thread(() -> {
            p.processPrepareResponse(new Event.PrepareResponse(1, 1, true, 0, 0, ByteString.EMPTY));
            p.processPrepareResponse(new Event.PrepareResponse(2, 1, true, 0, 0, ByteString.EMPTY));
            p.processPrepareResponse(new Event.PrepareResponse(3, 1, true, 0, 0, ByteString.EMPTY));
        }).start();

        Thread.sleep(10000);
    }
}
