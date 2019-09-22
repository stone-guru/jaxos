package org.axesoft.jaxos.base;

import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntBinaryOperator;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author gaoyuan
 * @sine 2018/7/2.
 */
public class SlideCounter {
    public enum StatMethod {
        STAT_MAX, STAT_MIN, STAT_SUM
    }

    private static final Map<StatMethod, Integer> INIT_VALUES = ImmutableMap.of(
            StatMethod.STAT_MAX, Integer.MIN_VALUE,
            StatMethod.STAT_MIN, Integer.MAX_VALUE,
            StatMethod.STAT_SUM, 0);

    private static class CounterNode {
        public CounterNode(long tinkle, int value, CounterNode next) {
            this.atomicInt = new AtomicInteger(value);
            this.tinkle = tinkle;
            this.next = next;
        }

        public final AtomicInteger atomicInt;
        public final long tinkle;
        public CounterNode next;
    }

    private AtomicReference<CounterNode> headRef;
    private AtomicInteger sizeRef;
    private AtomicBoolean truncating;
    private StatMethod statMethod;
    private final int unitSeconds;
    private final int minutes;
    private final int prefMaxSize;
    private final int initValue;

    public SlideCounter(int minutes, StatMethod statMethod, int unitSeconds) {
        checkArgument(minutes > 0);
        checkArgument(unitSeconds > 0 && unitSeconds <= 60);
        Objects.requireNonNull(statMethod);

        this.statMethod = statMethod;
        this.initValue = INIT_VALUES.get(this.statMethod);
        this.minutes = minutes;
        this.unitSeconds = unitSeconds;
        this.prefMaxSize = minutes * 60 / this.unitSeconds;
        this.headRef = new AtomicReference<>(new CounterNode(0, this.initValue, null));
        this.sizeRef = new AtomicInteger(1); //a init node at second 0
        this.truncating = new AtomicBoolean(false);
    }

    public void recordAtNow(int v) {
        recordAtSecond(System.currentTimeMillis() / 1000, v);
    }

    public void recordAtMillis(long millis, int v) {
        recordAtSecond(millis / 1000, v);
    }

    public void recordAtSecond(long timeSecond, int v) {
        CounterNode n = findOrAddNode(timeSecond);
        if (n != null) {
            acceptValue(n.atomicInt, v);
            if (this.sizeRef.get() >= prefMaxSize + prefMaxSize / 2) {
                truncate();
            }
        }
    }

    private CounterNode findOrAddNode(long timeSecond) {
        long tinkle = timeSecond / this.unitSeconds;
        while (true) {
            CounterNode head = headRef.get();
            if (head.tinkle == tinkle) {
                return head;
            }
            if (tinkle < head.tinkle) {
                if (head.tinkle - tinkle >= this.prefMaxSize) {
                    return null;
                }
                CounterNode node = head;
                while (node != null && node.tinkle > tinkle) {
                    node = node.next;
                }
                if (node != null && node.tinkle == tinkle) {
                    return node;
                }
                return null;
            }
            else {
                long t = (tinkle >= head.tinkle + prefMaxSize) ? tinkle - prefMaxSize + 1 : head.tinkle + 1;
                CounterNode nh = new CounterNode(t, this.initValue, head);
                if (headRef.compareAndSet(head, nh)) {
                    sizeRef.getAndIncrement();
                }
            }
        }
    }

    private void truncate() {
        boolean locked = truncating.compareAndSet(false, true);
        if (locked) {
            try {
                CounterNode n = headRef.get();
                CounterNode prev = null;
                long t = n.tinkle - this.prefMaxSize;
                while (n != null && n.tinkle > t) {
                    prev = n;
                    n = n.next;
                }

                if (prev != null) {
                    prev.next = null;
                    int k = 0;
                    while (n != null) {
                        k++;
                        n = n.next;
                    }
                    sizeRef.getAndAdd(-k);
                }
            }
            finally {
                truncating.set(false);
            }
        }
    }

    public int getSummary(long startSecond) {
        return fold(startSecond, 0, (a, b) -> a + b, 0);
    }

    public int getSummary() {
        return getSummary(System.currentTimeMillis() / 1000);
    }


    public int getMaximal(long startSecond, int deft) {
        return fold(startSecond, Integer.MIN_VALUE, Math::max, deft);
    }

    public int getMaximal(int deft) {
        return getMaximal(System.currentTimeMillis() / 1000, deft);
    }

    public int getMinimal(long startSecond, int deft) {
        return fold(startSecond, Integer.MAX_VALUE, Math::min, deft);
    }

    public int getMinimal(int deft) {
        return getMinimal(System.currentTimeMillis() / 1000, deft);
    }

    public double getMeanBySecond() {
        return getMeanBySecond(System.currentTimeMillis() / 1000);
    }

    public double getMeanBySecond(long startSecond) {
        double s = getSummary(startSecond);
        return s / (minutes * 60);
    }

    public int tinkleCount() {
        int k = 0;
        CounterNode n = headRef.get();
        while (n != null) {
            k++;
            n = n.next;
        }
        return k;
    }

    private void acceptValue(AtomicInteger atomicInt, int v) {
        switch (this.statMethod) {
            case STAT_MAX:
                atomicInt.updateAndGet(v0 -> Math.max(v0, v));
                return;
            case STAT_MIN:
                atomicInt.updateAndGet(v0 -> Math.min(v0, v));
                return;
            case STAT_SUM:
                atomicInt.getAndAdd(v);
        }
    }

    private int fold(long startSecond, int x0, IntBinaryOperator f, int deft) {
        long from = startSecond / unitSeconds;
        long to = from - (minutes * 60) / unitSeconds;

        CounterNode node = headRef.get();
        while (node != null && node.tinkle > from) {
            node = node.next;
        }

        int y = x0;
        boolean exists = false;
        while (node != null && node.tinkle > to) {
            int v = node.atomicInt.get();
            if(v != this.initValue) {
                y = f.applyAsInt(y, v);
                exists = true;
            }
            node = node.next;
        }

        return exists? y : deft;
    }

    private Date tinkleToDate(long tinkle){
        return new Date(tinkle * this.unitSeconds * 1000);
    }
}
