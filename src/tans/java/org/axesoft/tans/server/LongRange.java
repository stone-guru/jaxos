package org.axesoft.tans.server;

public class LongRange {
    private long low;
    private long high;

    public LongRange(long low, long high) {
        this.low = low;
        this.high = high;
    }

    public long low(){
        return this.low;
    }

    public long high(){
        return this.high;
    }
}
