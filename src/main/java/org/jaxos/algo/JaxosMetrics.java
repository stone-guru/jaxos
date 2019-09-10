package org.jaxos.algo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class JaxosMetrics {
    private AtomicLong proposeTimes = new AtomicLong(0);
    private AtomicLong proposeTotalNanos = new AtomicLong(0);

    public void recordPropose(long nanos){
        proposeTimes.incrementAndGet();
        proposeTotalNanos.addAndGet(nanos);
    }

    public long proposeTimes(){
        return proposeTimes.get();
    }

    public long proposeTotalNanos(){
        return proposeTotalNanos.get();
    }
    public long nanosPerPropose(){
        long i = proposeTimes.get();
        return proposeTotalNanos.get() / i;
    }
}
