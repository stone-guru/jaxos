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

    public int count() {
        return Long.bitCount(this.value);
    }

    public void clear() {
        value = 0;
    }

    /**
     * @return a new created set with save elements
     */
    public IntBitSet copy() {
        IntBitSet s = new IntBitSet();
        s.value = this.value;
        return s;
    }

    public void assign(IntBitSet s){
        this.value = s.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IntBitSet intBitSet = (IntBitSet) o;

        return value == intBitSet.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}

