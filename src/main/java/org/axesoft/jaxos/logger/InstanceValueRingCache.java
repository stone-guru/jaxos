package org.axesoft.jaxos.logger;

import com.google.common.collect.ImmutableList;
import org.axesoft.jaxos.algo.InstanceValue;

import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @sine 2019/10/15.
 */
public class InstanceValueRingCache {
    private InstanceValue[] buff;
    private int pos;

    public InstanceValueRingCache(int size) {
        checkArgument(size > 0, "RingBuff size should be positive %d", size);
        this.pos = 0;
        this.buff = new InstanceValue[size];
    }

    public synchronized void put(InstanceValue value) {
        int p0 = (this.pos - 1 + this.buff.length) % this.buff.length;
        InstanceValue v0 = this.buff[p0];
        if (v0 != null) {
            checkArgument(value.instanceId() >  v0.instanceId(),
                    "added instance id(%d) should great than prev(%d)",
                    value.instanceId(), v0.instanceId());
        }

        this.buff[this.pos] = value;
        this.pos = (this.pos + 1) % buff.length;
    }

    public synchronized int size() {
        if (buff[pos] == null) {
            return pos;
        }
        return buff.length;
    }

    public synchronized List<InstanceValue> get(long idLow, long idHigh) {
        int i0 = lessOrEqualPosOf(idHigh);

        if (i0 < 0) {
            return Collections.emptyList();
        }

        ImmutableList.Builder<InstanceValue> builder = ImmutableList.builder();

        int i = i0;
        do {
            InstanceValue v = this.buff[i];
            if (v == null || v.instanceId < idLow) {
                break;
            }
            else {
                builder.add(this.buff[i]);
                i = (i - 1 + this.buff.length) % this.buff.length;
            }
        } while (i != i0);

        return builder.build().reverse();
    }

    private int lessOrEqualPosOf(long instanceId) {
        int i = (this.pos - 1 + this.buff.length) % this.buff.length;

        while (true) {
            InstanceValue v = this.buff[i];
            if (v == null) {
                return -1;
            }
            else if (v.instanceId <= instanceId) {
                return i;
            }
            else if (i == this.pos) {
                return -1;
            }
            else {
                i = (i - 1 + this.buff.length) % this.buff.length;
            }
        }
    }
}
