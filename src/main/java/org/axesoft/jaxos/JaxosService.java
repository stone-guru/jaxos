package org.axesoft.jaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.netty.util.Timeout;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.algo.*;
import org.axesoft.jaxos.logger.LevelDbAcceptorLogger;
import org.axesoft.jaxos.algo.EventWorkerPool;
import org.axesoft.jaxos.netty.NettyCommunicatorFactory;
import org.axesoft.jaxos.netty.NettyJaxosServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
    private Platoon platoon;

    private ScheduledExecutorService timerExecutor;
    private Configuration configuration;

    public JaxosService(JaxosSettings settings, StateMachine stateMachine) {
        this.settings = checkNotNull(settings, "settings is null");
        this.stateMachine = stateMachine;
        this.acceptorLogger = new LevelDbAcceptorLogger(this.settings.dbDirectory());
        //this.acceptorLogger = new FileAcceptorLogger(this.settings.dbDirectory(), settings.partitionNumber());

        this.configuration = new Configuration() {
            @Override
            public Communicator getCommunicator() {
                return JaxosService.this.communicator;
            }

            @Override
            public AcceptorLogger getLogger() {
                return JaxosService.this.acceptorLogger;
            }

            @Override
            public EventWorkerPool getWorkerPool() {
                return JaxosService.this.eventWorkerPool;
            }

            @Override
            public EventTimer getEventTimer() {
                return JaxosService.this.eventWorkerPool;
            }
        };

        checkArgument(settings.partitionNumber() >= 0, "Invalid partition number %d", settings.partitionNumber());
        this.squads = new Squad[settings.partitionNumber()];
        for (int i = 0; i < settings.partitionNumber(); i++) {
            final int n = i;
            this.squads[i] = new Squad(n, settings, this.configuration, stateMachine);
            this.squads[i].restoreFromDB(); //FIXME it can run in parallel
        }

        this.platoon = new Platoon();

        this.timerExecutor = Executors.newScheduledThreadPool(1, (r) -> {
            String name = "scheduledTaskThread";
            Thread thread = new Thread(r, name);
            thread.setDaemon(true);
            return thread;
        });

        super.addListener(new JaxosServiceListener(), MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Void> propose(int squadId, long instanceId, ByteString v) {
        return propose(squadId, instanceId, new Event.BallotValue(Event.ValueType.APPLICATION, v));
    }

    public ListenableFuture<Void> propose(int squadId, long instanceId, Event.BallotValue v) {
        if (!this.isRunning()) {
            throw new IllegalStateException(SERVICE_NAME + " is not running");
        }
        checkArgument(squadId >= 0 && squadId < squads.length,
                "Invalid squadId(%s) while partition number is %s ", squadId, squads.length);

        SettableFuture<Void> resultFuture = SettableFuture.create();

        eventWorkerPool.queueBallotTask(squadId,
                () -> this.squads[squadId].propose(instanceId, v, resultFuture));

        return resultFuture;
    }

    public void printMetrics() {
        long current = System.currentTimeMillis();
        for (Squad squad : this.squads) {
            squad.computeAndPrintMetrics(current);
        }
    }

    @Override
    protected void triggerShutdown() {
        this.timerExecutor.shutdownNow();
        this.communicator.close();
        this.node.shutdown();
        this.stateMachine.close();
        this.acceptorLogger.close();
        this.eventWorkerPool.shutdown();
    }

    @Override
    protected void run() throws Exception {
        this.eventWorkerPool = new EventWorkerPool(settings.partitionNumber(), () -> this.platoon);
        this.node = new NettyJaxosServer(this.settings, this.eventWorkerPool);

        NettyCommunicatorFactory factory = new NettyCommunicatorFactory(settings, this.eventWorkerPool);
        this.communicator = factory.createCommunicator();

        //this.timerExecutor.scheduleWithFixedDelay(this::saveCheckPoint, 10, 60 * settings.checkPointMinutes(), TimeUnit.SECONDS);
        //this.timerExecutor.scheduleWithFixedDelay(platoon::startChosenQuery, 10, 10, TimeUnit.SECONDS);
        //this.timerExecutor.scheduleAtFixedRate(this::runForLeader, 3, 1, TimeUnit.SECONDS);
        this.timerExecutor.scheduleAtFixedRate(new RunnableWithLog(logger, () -> this.acceptorLogger.sync()), 2000, 500, TimeUnit.MILLISECONDS);
        this.node.startup();
    }

    public ScheduledExecutorService timerExecutor() {
        return this.timerExecutor;
    }

    private void runForLeader() {
        int mySquadCount = 0;
        int responsibility = this.squads.length / this.settings.peerCount();
        if (responsibility == 0) {
            responsibility = 1;
        }
        int extra = this.squads.length - (responsibility * this.settings.peerCount()) > 0 ? 1 : 0;

        for (int i = 0; i < this.squads.length; i++) {
            SquadContext context = squads[i].context();
            if (context.isLeader()) {
                mySquadCount++;
                if (System.currentTimeMillis() - context.lastSuccessAccept().timestampMillis() >= this.settings.leaderLeaseSeconds() * 500) {
                    proposeForLeader(i);
                }
            }
        }

        int j = 0;
        while (mySquadCount < responsibility + extra && j < this.squads.length) {
            if (!squads[j].context().isOtherLeaderActive()) {
                proposeForLeader(j);
                mySquadCount++;
            }
            j++;
        }
    }

    private void proposeForLeader(int squadId) {
        final ListenableFuture<Void> future = this.propose(squadId, squads[squadId].lastChosenInstanceId() + 1, Event.BallotValue.EMPTY);
        future.addListener(() -> {
            try {
                future.get();
                if (logger.isTraceEnabled()) {
                    logger.trace("S{} Emphasis leader again", squadId);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                logger.debug("S{} Failed to be leader", squadId);
            }
        }, MoreExecutors.directExecutor());
    }

    private void saveCheckPoint() {
        for (Squad squad : this.squads) {
            try {
                squad.saveCheckPoint();
            }
            catch (Exception e) {
                if (e.getCause() instanceof InterruptedException) {
                    logger.info("Save checkpoint S{} interrupted", squad.id());
                    return;
                }
                logger.error("Save checkpoint S{} error", squad.id());
            }
        }
    }

    private class Platoon implements EventDispatcher {
        /**
         * Map(SquadId, Pair(ServerId, last chosen instance id))
         */
        private Map<Integer, Pair<Integer, Long>> squadInstanceMap = new HashMap<>();
        private int chosenQueryResponseCount = 0;
        private Timeout chosenQueryTimeout;

        @Override
        public Event processEvent(Event event) {
            switch (event.code()) {
                case CHOSEN_QUERY: {
                    return makeChosenQueryResponse();
                }
                case CHOSEN_QUERY_RESPONSE: {
                    onChosenQueryResponse((Event.ChosenQueryResponse) event);
                    if (this.chosenQueryResponseCount == settings.peerCount() - 1) {
                        chosenQueryTimeout.cancel();
                        endChosenQueryResponse();
                    }
                    return null;
                }
                case CHOSEN_QUERY_TIMEOUT: {
                    endChosenQueryResponse();
                    return null;
                }
                default: {
                    return getSquad(event).processEvent(event);
                }
            }
        }

        private Squad getSquad(Event event) {
            final int squadId = event.squadId();
            if (squadId >= 0 && squadId < JaxosService.this.squads.length) {
                return JaxosService.this.squads[squadId];
            }
            else {
                throw new IllegalArgumentException("Invalid squadId in " + event.toString());
            }
        }

        private Event.ChosenQueryResponse makeChosenQueryResponse() {
            ImmutableList.Builder<Pair<Integer, Long>> builder = ImmutableList.builder();
            for (int i = 0; i < JaxosService.this.squads.length; i++) {
                builder.add(Pair.of(i, JaxosService.this.squads[i].lastChosenInstanceId()));
            }
            Event.ChosenQueryResponse r = new Event.ChosenQueryResponse(settings.serverId(), builder.build());

            if (logger.isTraceEnabled()) {
                logger.trace("Generate {}", r);
            }
            return r;
        }

        private void startChosenQuery() {
            this.squadInstanceMap.clear();
            this.chosenQueryResponseCount = 0;

            if (settings.peerCount() == 1) {
                return;
            }

            configuration.getCommunicator().broadcastOthers(new Event.ChosenQuery(settings.serverId()));
            this.chosenQueryTimeout = configuration.getEventTimer().createTimeout(100, TimeUnit.MILLISECONDS,
                    new Event.ChosenQueryTimeout(settings.serverId()));
        }

        private void onChosenQueryResponse(Event.ChosenQueryResponse response) {
            this.chosenQueryResponseCount++;
            for (Pair<Integer, Long> p : response.squadChosen()) {
                this.squadInstanceMap.merge(p.getKey(), Pair.of(response.senderId(), p.getRight()),
                        (p0, p1) -> {
                            if (p0.getRight() >= p1.getRight()) {
                                return p0;
                            }
                            else {
                                return p1;
                            }
                        });
            }
        }

        private void endChosenQueryResponse() {
            for (Map.Entry<Integer, Pair<Integer, Long>> entry : squadInstanceMap.entrySet()) {
                int squadId = entry.getKey();
                int serverId = entry.getValue().getKey();
                long instanceId = entry.getValue().getValue();

                Pair<Integer, Long> p = Pair.of(squadId, instanceId);
                Event.ChosenQueryResponse response = new Event.ChosenQueryResponse(serverId, ImmutableList.of(p));
                configuration.getWorkerPool().queueInstanceTask(() ->
                        JaxosService.this.squads[squadId].processEvent(response));
            }
        }
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
