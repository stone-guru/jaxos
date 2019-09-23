package org.axesoft.jaxos.base;

public class KeyLong {
    private String key;
    private long value;

    public KeyLong(String key, long value) {
        this.key = key;
        this.value = value;
    }

    public String key(){
        return this.key;
    }

    public long value(){
        return this.value;
    }
}
