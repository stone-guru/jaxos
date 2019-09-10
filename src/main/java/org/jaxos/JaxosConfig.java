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
        private int httpPort;

        public Peer(int id, String address, int port, int httpPort) {
            this.id = id;
            this.address = address;
            this.port = port;
            this.httpPort = httpPort;
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

        public int httpPort(){
            return httpPort;
        }

        @Override
        public String toString() {
            return "Peer{" +
                    "id=" + id +
                    ", address='" + address + '\'' +
                    ", port=" + port +
                    ", httpPort=" + httpPort +
                    '}';
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int serverId;
        private Peer self;

        private ImmutableMap.Builder<Integer, Peer> peerBuilder = ImmutableMap.<Integer, Peer>builder();

        public Builder setServerId(int serverId) {
            this.serverId = serverId;
            return this;
        }

        public Builder setSelf(Peer self) {
            this.self = self;
            return this;
        }

        public Builder addPeer(Peer peer){
            peerBuilder.put(peer.id(), peer);
            return this;
        }

        public JaxosConfig build(){
            JaxosConfig config = new JaxosConfig();
            config.serverId = this.serverId;
            config.self = this.self;
            config.peerMap = this.peerBuilder.build();
            config.leaderLeaseSeconds = 30;
            return config;
        }
    }

    private Map<Integer, Peer> peerMap;
    private int serverId;
    private Peer self;
    private int leaderLeaseSeconds;

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

    public Peer self() {
        return this.self;
    }

    public int peerCount() {
        return this.peerMap.size() + 1;
    }

    public int leaderLeaseSeconds(){
        return this.leaderLeaseSeconds;
    }

    @Override
    public String toString() {
        return "JaxosConfig{" +
                "peerMap=" + peerMap +
                ", serverId=" + serverId +
                ", self=" + self +
                ", leaderLeaseSeconds=" + leaderLeaseSeconds +
                '}';
    }
}
