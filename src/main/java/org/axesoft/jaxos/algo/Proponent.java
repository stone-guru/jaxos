package org.axesoft.jaxos.algo;

import com.google.protobuf.ByteString;

public interface Proponent {
    ProposeResult propose(ByteString v) throws InterruptedException;
}
