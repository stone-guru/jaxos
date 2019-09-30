package org.axesoft.tans.client;

import io.netty.util.concurrent.Future;
import org.axesoft.jaxos.base.LongRange;


public interface TansClient {
    Future<LongRange> acquire(String key, int n);
    void close();
}
