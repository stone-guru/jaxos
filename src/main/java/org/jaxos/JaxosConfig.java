package org.jaxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class JaxosConfig {
    public static class Peer {
        private int id;
        private String address;
        private int port;

        public Peer(int id, String address, int port) {
            this.id = id;
            this.address = address;
            this.port = port;
        }

        public int id() {
            return id;
        }

        public String address() {
            return address;
        }

        public int port() {
            return port;
        }
    }

    private Map<Integer, Peer> peerMap;

    public JaxosConfig() {
        this.peerMap = ImmutableMap.<Integer, Peer>builder().put(1, new Peer(1, "127.0.0.1", 9999)).build();
    }

    public Peer getPeer(int id){
        return peerMap.get(id);
    }

    public Map<Integer, Peer> peerMap() {
        return this.peerMap;
    }
}
