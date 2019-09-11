package org.axesoft.jaxos.base;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gaoyuan
 * @sine 2019/8/31.
 */
public class EntryOnce {
    private AtomicInteger lock = new AtomicInteger(0);

    public boolean exec(Runnable r) {
        if (lock.compareAndSet(0, 1)) {
            try {
                r.run();
                return true;
            }
            finally {
                lock.set(0);
            }
        }
        else {
            return false;
        }
    }
}
