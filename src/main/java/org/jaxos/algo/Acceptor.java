package org.jaxos.algo;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author gaoyuan
 * @sine 2019/8/22.
 */
public class Acceptor {

    public Pair<Integer, byte[]> prepare(long instanceId, int ballot){
        return null;
    }

    public long accept(long instanceId, int ballot, byte[] value){
        return 0L;
    }
}
