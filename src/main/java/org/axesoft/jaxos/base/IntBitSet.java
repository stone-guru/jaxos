package org.axesoft.jaxos.base;

/**
 * @author gaoyuan
 * @sine 2019/8/28.
 */
public class IntBitSet {
    private long value = 0;

    public void add(int i) {
        long mask = 1L << i;
        this.value = this.value | mask;
    }

    public boolean get(int i) {
        long mask = 1L << i;
        return (value & mask) != 0;
    }

    public int count(){
        return Long.bitCount(this.value);
    }

    public void clear() {
        value = 0;
    }
}

