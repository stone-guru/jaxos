package org.axesoft.jaxos.base;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author gaoyuan
 * @sine 2019/9/22.
 */
public class Derivator {
    long v0;
    long t0;

    public Derivator() {
        System.currentTimeMillis();
    }

    public Derivator(long now) {
        this.t0 = now;
    }

    /**
     * Accept new value and compute the delta and derivation
     *
     * @param v1      new value
     * @param current new timestamp
     * @return Pair of value delta, and derivation
     */
    public synchronized Pair<Double, Double> accept(long v1, long current) {
        long t1 = current;
        double delta = v1 - this.v0;

        double t = (t1 - this.t0) / 1000.0;
        double d = delta / t;

        this.v0 = v1;
        this.t0 = t1;

        return Pair.of(delta, d);
    }
}
