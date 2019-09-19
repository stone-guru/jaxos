package org.axesoft.jaxos;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.logger.LevelDbAcceptorLogger;
import org.axesoft.jaxos.algo.EventWorkerPool;
import org.axesoft.jaxos.netty.NettyCommunicatorFactory;
import org.axesoft.jaxos.netty.NettyJaxosServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

public class JaxosService extends AbstractExecutionThreadService implements Proponent {
    public static final String SERVICE_NAME = "Jaxos service";

    private static Logger logger = LoggerFactory.getLogger(JaxosService.class);


    private JaxosSettings settings;
    private StateMachine stateMachine;
    private AcceptorLogger acceptorLogger;
    private Communicator communicator;
    private NettyJaxosServer node;

    private EventWorkerPool eventWorkerPool;
    private Squad[] squads;
    private EventDispatcher platoonEventDispatcher;

    public JaxosService(JaxosSettings settings, StateMachine stateMachine) {
        this.settings = settings;
        this.stateMachine = stateMachine;
        this.acceptorLogger = new LevelDbAcceptorLogger(this.settings.dbDirectory());

        this.squads = new Squad[settings.partitionNumber()];
        for (int i = 0; i < settings.partitionNumber(); i++) {
            final int n = i;
            this.squads[i] = new Squad(n, settings, () -> communicator, acceptorLogger, stateMachine, () -> this.eventWorkerPool);
        }

        this.platoonEventDispatcher = (event) -> {
            if (event.squadId() >= 0 && event.squadId() < squads.length) {
                Squad squad = squads[event.squadId()];
                return squad.process(event);
            }
            else {
                throw new IllegalArgumentException("Invalid squadId in " + event.toString());
            }
        };

        super.addListener(new JaxosServiceListener(), MoreExecutors.directExecutor());
    }


    @Override
    public ProposeResult propose(int squadId, long instanceId, ByteString v) throws InterruptedException {
        if (!this.isRunning()) {
            throw new IllegalStateException(SERVICE_NAME + " is not running");
        }
        checkArgument(squadId >= 0 && squadId < squads.length,
                "Invalid squadId(%s) while partition number is %s ", squadId, squads.length);

        return this.squads[squadId].propose(instanceId, v);
    }

    @Override
    protected void triggerShutdown() {
        this.communicator.close();
        this.node.shutdown();
        this.stateMachine.close();
        this.acceptorLogger.close();
    }

    @Override
    protected void run() throws Exception {
        this.eventWorkerPool = new EventWorkerPool(settings.partitionNumber(), () -> this.platoonEventDispatcher);
        this.node = new NettyJaxosServer(this.settings, this.eventWorkerPool);

        NettyCommunicatorFactory factory = new NettyCommunicatorFactory(settings, this.eventWorkerPool);
        this.communicator = factory.createCommunicator();

        this.node.startup();
    }

    private class JaxosServiceListener extends Listener {
        @Override
        public void running() {
            logger.info("{} {} started at port {}", SERVICE_NAME, settings.serverId(), settings.self().port());
        }

        @Override
        public void stopping(State from) {
            logger.info("{} is stopping", SERVICE_NAME);
        }

        @Override
        public void terminated(State from) {
            logger.info("{} terminated from {}", SERVICE_NAME, from);
        }
    }
}
