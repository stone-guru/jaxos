package org.axesoft.jaxos.algo;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/9/17.
 */
public class EventWorkerPool implements EventTimer {
    private static Logger logger = LoggerFactory.getLogger(EventWorkerPool.class);

    private static final int THREAD_QUEUE_CAPACITY = 10 * 1024;
    private ExecutorService[] ballotExecutors;
    private Supplier<EventDispatcher> eventDispatcherSupplier;
    private HashedWheelTimer timer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);

    public EventWorkerPool(int threadNum, Supplier<EventDispatcher> eventDispatcherSupplier) {
        this.eventDispatcherSupplier = eventDispatcherSupplier;
        this.ballotExecutors = new ExecutorService[threadNum];
        for (int i = 0; i < threadNum; i++) {
            final int threadNo = i;
            this.ballotExecutors[i] = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(THREAD_QUEUE_CAPACITY),
                    (r) -> {
                        String name = "EventWorkerThread-" + threadNo;
                        Thread thread = new Thread(r, name);
                        thread.setDaemon(true);
                        return thread;
                    });
        }
    }

    public void queueBallotTask(int squadId, Runnable r) {
        int n = squadId % ballotExecutors.length;
        this.ballotExecutors[n].submit(new RunnableWithLog(squadId, logger, r));
    }


    public void queueInstanceTask(Runnable r) {
        this.ballotExecutors[0].submit(new RunnableWithLog(logger, r));
    }

    public void submitEventToSelf(Event event) {
        this.submitEvent(event, this::submitEventToSelf);
    }

    public void submitEvent(Event event, Consumer<Event> resultConsumer) {
        int n = event.squadId() % ballotExecutors.length;
        ExecutorService executor = this.ballotExecutors[n];

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

    public void shutdown() {
        for (ExecutorService executor : this.ballotExecutors) {
            executor.shutdownNow();
        }
    }
}
