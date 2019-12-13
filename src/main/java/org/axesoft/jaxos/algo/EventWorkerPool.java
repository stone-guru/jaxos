package org.axesoft.jaxos.algo;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/9/17.
 */
public class EventWorkerPool implements EventTimer {
    private static Logger logger = LoggerFactory.getLogger(EventWorkerPool.class);

    private ExecutorService[] ballotExecutors;
    private ExecutorService learnerExecutor;
    private Supplier<EventDispatcher> eventDispatcherSupplier;
    private HashedWheelTimer timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);

    public EventWorkerPool(int threadNum, Supplier<EventDispatcher> eventDispatcherSupplier) {
        this.eventDispatcherSupplier = eventDispatcherSupplier;
        this.ballotExecutors = new ExecutorService[threadNum];
        for (int i = 0; i < threadNum; i++) {
            final int threadNo = i;
            this.ballotExecutors[i] = Executors.newSingleThreadExecutor((r) -> {
                String name = "EventWorkerThread-" + threadNo;
                Thread thread = new Thread(r, name);
                thread.setDaemon(true);
                return thread;
            });
        }

        this.learnerExecutor = Executors.newSingleThreadExecutor((r) -> {
            String name = "LearnerThread";
            Thread thread = new Thread(r, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    public void queueBallotTask(int squadId, Runnable r) {
        int n = squadId % ballotExecutors.length;
        this.ballotExecutors[n].submit(new RunnableWithLog(squadId, logger, r));
    }


    public void queueInstanceTask(Runnable r) {
        this.learnerExecutor.submit(new RunnableWithLog(logger, r));
    }

    public void submitEventToSelf(Event event) {
        this.submitEvent(event, this::submitEventToSelf);
    }

    public void submitEvent(Event event, Consumer<Event> resultConsumer) {
        ExecutorService executor;
        int n = event.squadId() % ballotExecutors.length;
        executor = this.ballotExecutors[n];

//        if (event instanceof Event.BallotEvent) {
//            int n = event.squadId() % ballotExecutors.length;
//            executor = this.ballotExecutors[n];
//        }
//        else if (event instanceof Event.InstanceEvent) {
//            executor = this.learnerExecutor;
//        }
//        else {
//            throw new UnsupportedOperationException(event.getClass().getName());
//        }

        executor.submit(new RunnableWithLog(event.squadId(), logger,
                () -> {
                    Event result = this.eventDispatcherSupplier.get().processEvent(event);
                    if (result != null) {
                        resultConsumer.accept(result);
                    }
                }));
    }

    public void directCallSelf(Event event) {
        this.eventDispatcherSupplier.get().processEvent(event);
    }

    @Override
    public Timeout createTimeout(long delay, TimeUnit timeUnit, Event timeoutEvent) {
        return timer.newTimeout((t) -> this.submitEventToSelf(timeoutEvent), delay, timeUnit);
    }

    public ExecutorService learnerExecutor() {
        return this.learnerExecutor;
    }

    public void shutdown() {
        learnerExecutor.shutdownNow();
        for (ExecutorService executor : this.ballotExecutors) {
            executor.shutdownNow();
        }
    }
}
