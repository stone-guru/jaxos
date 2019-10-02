package org.axesoft.jaxos.algo;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

public interface Proponent {
    ListenableFuture<Void> propose(int squadId, long instanceId, ByteString v);
}
