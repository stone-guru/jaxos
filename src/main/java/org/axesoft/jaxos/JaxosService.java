package org.axesoft.jaxos;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.logger.BerkeleyDbAcceptorLogger;
import org.axesoft.jaxos.netty.NettyCommunicatorFactory;
import org.axesoft.jaxos.netty.NettyJaxosNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JaxosService extends AbstractExecutionThreadService {
    private static Logger logger = LoggerFactory.getLogger(JaxosService.class);

    private JaxosSettings settings;
    private StateMachine machine;
    private Squad squad;
    private AcceptorLogger acceptorLogger;
    private Communicator communicator;
    private NettyJaxosNode node;

    public JaxosService(JaxosSettings settings, StateMachine machine) {
        this.settings = settings;
        this.machine = machine;
        this.acceptorLogger = new BerkeleyDbAcceptorLogger(this.settings.dbDirectory());
        this.squad = new Squad(1, settings, () -> communicator, acceptorLogger);
        super.addListener(new JaxosServiceListener(), MoreExecutors.directExecutor());
    }

    public Proponent getProponent(){
        return this.squad;
    }

    @Override
    protected void triggerShutdown() {
        this.node.shutdown();
        this.acceptorLogger.close();
    }

    @Override
    protected void run() throws Exception {
        this.node = new NettyJaxosNode(this.settings, () -> this.squad);

        NettyCommunicatorFactory factory = new NettyCommunicatorFactory(settings, squad);
        this.communicator = factory.createCommunicator();

        this.node.startup();
    }

    private class JaxosServiceListener extends Listener {
        @Override
        public void running() {
            logger.info("Jaxos server {} started at port {}", settings.serverId(), settings.self().port());
        }

        @Override
        public void stopping(State from) {
            logger.info("stopping jaxos server");
        }

        @Override
        public void terminated(State from) {
            logger.info("jaxos server terminated from " + from);
        }
    }
}
