package org.jaxos.base;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/8/28.
 */
public class IntBitSet {
    private AtomicLong value = new AtomicLong(0);

    public void add(int i) {
        long mask = 1L << i;
        long v0, v1;
        do {
            v0 = value.get();
            v1 = v0 | mask;
        } while (!value.compareAndSet(v0, v1));
    }

    public boolean get(int i) {
        long mask = 1L << i;
        return (value.get() & mask) > 0;
    }

    public int count(){
        long n = value.get();
        return Long.bitCount(n);
    }

    public void clear() {
        value.set(0L);
    }
}

