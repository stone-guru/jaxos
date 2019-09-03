package org.jaxos.algo;

import com.google.protobuf.ByteString;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author gaoyuan
 * @sine 2019/9/2.
 */
public class InstanceContext {
    private AtomicLong lastInstanceId = new AtomicLong(0);
    private ConcurrentMap<Long, ByteString> histValues = new ConcurrentHashMap<>();

    public long lastInstanceId() {
        return lastInstanceId.get();
    }

    public ByteString valueOf(long instanceId){
        return histValues.getOrDefault(instanceId, ByteString.EMPTY);
    }

    public void learnValue(long instanceId, ByteString value) {
        if (this.histValues.put(instanceId, value) == null) {
            for (; ; ) {
                long i0 = this.lastInstanceId.get();
                if (instanceId > i0) {
                    if(this.lastInstanceId.compareAndSet(i0, instanceId)){
                        break;
                    }
                }
            }
        }
    }
}
