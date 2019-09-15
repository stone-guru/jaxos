package org.axesoft.tans.statemachine;

public class TansNumber {
    private String name;
    private long version;
    private long timestamp;
    private long value;

    public TansNumber(String name, long value){
        this(name, 0, System.currentTimeMillis(), value);
    }

    public TansNumber(String name, long version, long timestamp, long value) {
        this.name = name;
        this.version = version;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String name(){
        return this.name;
    }

    public long version(){
        return this.version;
    }

    public long timestamp(){
        return this.timestamp;
    }

    public long value(){
        return value;
    }

    public TansNumber update(long v){
        return new TansNumber(this.name, this.version + 1, System.currentTimeMillis(), this.value + v);
    }
}
