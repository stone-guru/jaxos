/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.jaxos.app;

import org.jaxos.JaxosConfig;
import org.jaxos.algo.Acceptor;
import org.jaxos.algo.Communicator;
import org.jaxos.algo.LocalEventCenter;
import org.jaxos.algo.Proposal;
import org.jaxos.netty.NettyCommunicatorFactory;

import java.util.Date;

public class ClientApp {

    public static void main(String[] args) throws Exception {
        JaxosConfig config = new ArgumentParser().parse(args);
        ClientApp app = new ClientApp(config);
        app.run();
    }


    private JaxosConfig config;
    private Communicator communicator;

    public ClientApp(JaxosConfig config) {
        this.config = config;
    }

    public void run() throws Exception {
        Acceptor acceptor = new Acceptor(config);
        Proposal proposal = new Proposal(config, () -> communicator);
        NettyCommunicatorFactory factory = new NettyCommunicatorFactory(config, new LocalEventCenter(proposal, acceptor));
        this.communicator = factory.createCommunicator();

        final int n = 100;
        for(int i = 0; i < n; i++) {
            byte[] s = new Date().toString().getBytes("UTF-8");
            proposal.propose(s);

            while(!proposal.accepted()) {
                Thread.sleep(5);
            }
        }
        Thread.sleep(1000);

        double t = (double)proposal.taskElpasedMillis() - 200;
        System.out.println("Average time is " + t/(n-1));
        this.communicator.close();
    }
}
