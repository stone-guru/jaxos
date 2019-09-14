package org.axesoft.jaxos.algo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/7.
 */
public class JaxosMetrics {
    private AtomicLong proposeTimes = new AtomicLong(0);
    private AtomicLong proposeTotalNanos = new AtomicLong(0);
    private AtomicLong successTimes = new AtomicLong(0);
    private AtomicLong conflictTimes = new AtomicLong(0);

    public void recordPropose(long nanos, ProposeResult r){
        proposeTimes.incrementAndGet();
        proposeTotalNanos.addAndGet(nanos);

        switch (r.code()){
            case SUCCESS:
                successTimes.incrementAndGet();
                break;
            case CONFLICT:
                conflictTimes.incrementAndGet();
                break;
        }
    }

    public long proposeTimes(){
        return proposeTimes.get();
    }

    public long proposeTotalNanos(){
        return proposeTotalNanos.get();
    }

    public long successTimes(){
        return successTimes.get();
    }

    public long conflictTimes(){
        return conflictTimes.get();
    }

    public long nanosPerPropose(){
        long i = proposeTimes.get();
        return proposeTotalNanos.get() / i;
    }
}
