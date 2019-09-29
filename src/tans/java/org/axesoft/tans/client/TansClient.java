package org.axesoft.tans.client;

import org.axesoft.jaxos.base.LongRange;

import java.util.concurrent.TimeoutException;

public interface TansClient {
    LongRange acquire(String key, int n) throws TimeoutException;
    void close();
}
