package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface Proponent {
    ProposeResult propose(long instanceId, ByteString v) throws InterruptedException;
}
