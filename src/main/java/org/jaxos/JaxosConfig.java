package org.jaxos;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int serverId;
        private int port;
        private ImmutableMap.Builder<Integer, Peer> peerBuilder = ImmutableMap.<Integer, Peer>builder();

        public Builder setServerId(int serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder addPeer(int id, String address, int port){
            this.peerBuilder.put(id, new Peer(id, address, port));
            return this;
        }

        public JaxosConfig build(){
            JaxosConfig config = new JaxosConfig();
            config.port = this.port;
            config.serverId = this.serverId;
            config.peerMap = this.peerBuilder.build();
            return config;
        }
    }


    private Map<Integer, Peer> peerMap;
    private int serverId;
    private int port;

    private JaxosConfig() {
    }

    public Peer getPeer(int id){
        return peerMap.get(id);
    }

    public Map<Integer, Peer> peerMap() {
        return this.peerMap;
    }

    public int serverId() {
        return this.serverId;
    }

    public int port() {
        return this.port;
    }
}
