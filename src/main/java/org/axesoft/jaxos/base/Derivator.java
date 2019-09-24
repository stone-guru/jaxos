package org.axesoft.jaxos.base;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/22.
 */
public class Derivator {
    private long v0;
    private long t0;
    private AtomicLong value = new AtomicLong(0);

    public Derivator() {
        this(System.currentTimeMillis());
    }

    public Derivator(long now) {
        this.t0 = now;
    }

    public void inc(){
        this.value.incrementAndGet();
    }

    public Pair<Double, Double> compute(long current){
        return compute(value.get(), current);
    }

    /**
     * Accept new value and compute the delta and derivation
     *
     * @param current new timestamp
     * @return Pair of value delta, and derivation
     */
    public synchronized Pair<Double, Double> compute(long v1, long current) {
        long t1 = current;
        double delta = v1 - this.v0;

        double t = (t1 - this.t0) / 1000.0;
        double d = delta / t;

        this.v0 = v1;
        this.t0 = t1;

        return Pair.of(delta, d);
    }
}
