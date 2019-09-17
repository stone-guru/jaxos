package org.axesoft.jaxos.algo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author gaoyuan
 * @sine 2019/9/17.
 */
public class EventWorkerPool {

    private ExecutorService[] executors;
    private Supplier<EventDispatcher> eventDispatcherSupplier;

    private AtomicInteger threadNumber = new AtomicInteger(0);

    public EventWorkerPool(int threadNum, Supplier<EventDispatcher> eventDispatcherSupplier) {
        this.eventDispatcherSupplier = eventDispatcherSupplier;
        this.executors = new ExecutorService[threadNum];
        for (int i = 0; i < threadNum; i++) {
            this.executors[i] = Executors.newSingleThreadExecutor((r) -> {
                String name = "EventWorkerThread-" + threadNumber.incrementAndGet();
                Thread thread = new Thread(r, name);
                thread.setDaemon(true);
                return thread;
            });
        }
    }

    public void submitToSelf(Event event) {
        this.submit(event, this::submitToSelf);
    }

    public void submit(Event event, Consumer<Event> resultConsumer) {
        int n = event.squadId() % executors.length;

        this.executors[n].submit(() -> {
            Event result = this.eventDispatcherSupplier.get().process(event);
            if (result != null) {
                resultConsumer.accept(result);
            }
        });
    }
}
