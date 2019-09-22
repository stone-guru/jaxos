package org.axesoft.jaxos.base;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/22.
 */
public class Velometer {
    private AtomicLong times = new AtomicLong(0);
    private AtomicLong totalNanos = new AtomicLong(0);

    private volatile long lastTimestamp;
    private volatile long lastTimes = 0;
    private long lastNanos = 0;

    public Velometer(){
        this(System.currentTimeMillis());
    }

    public Velometer(long now) {
        this.lastTimestamp = now;
    }

    public void record(long nanos) {
        times.incrementAndGet();
        totalNanos.addAndGet(nanos);
    }

    public synchronized double compute(long timestamp) {
        if (timestamp <= this.lastTimestamp) {
            return 0D;
        }

        long t1 = times.get();
        long n1 = totalNanos.get();

        double td = (t1 - lastTimes);
        double nd = (n1 - lastNanos)/1e+6;//nano to milli seconds
        double r = nd / td;

        this.lastTimes = t1;
        this.lastNanos = n1;
        this.lastTimestamp = timestamp;
        return r;
    }

    public long times(){
        return times.get();
    }

    public long timesDelta(){
        return times.get() - lastTimes;
    }

    public long lastTimestamp() {
        return this.lastTimestamp;
    }
}
