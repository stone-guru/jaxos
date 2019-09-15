package org.axesoft.jaxos;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * @author gaoyuan
 * @sine 2019/8/24.
 */
public class JaxosSettings {
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

        @Override
        public String toString() {
            return "Peer{" +
                    "id=" + id +
                    ", address='" + address + '\'' +
                    ", port=" + port +
                    '}';
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int serverId;
        private Peer self;
        private boolean ignoreLeader;
        private String dbDirectory;
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

        public Builder setIgnoreLeader(boolean ignoreLeader) {
            this.ignoreLeader = ignoreLeader;
            return this;
        }

        public Builder setDbDirectory(String dbDirectory) {
            this.dbDirectory = dbDirectory;
            return this;
        }

        public JaxosSettings build(){
            JaxosSettings config = new JaxosSettings();
            config.serverId = this.serverId;
            config.peerMap = this.peerBuilder.build();
            config.ignoreLeader = this.ignoreLeader;
            config.leaderLeaseSeconds = 3;
            config.dbDirectory = this.dbDirectory;
            return config;
        }
    }

    private Map<Integer, Peer> peerMap;
    private int serverId;
    private int leaderLeaseSeconds;
    private boolean ignoreLeader;
    private String dbDirectory;

    private JaxosSettings() {
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
        return this.peerMap.get(this.serverId);
    }

    public int peerCount() {
        return this.peerMap.size();
    }

    public int leaderLeaseSeconds(){
        return this.leaderLeaseSeconds;
    }

    public boolean ignoreLeader(){
        return this.ignoreLeader;
    }

    public String dbDirectory(){
        return this.dbDirectory;
    }

    public boolean reachQuorum(int n){
        return n > this.peerMap.size() / 2;
    }

    @Override
    public String toString() {
        return "JaxosConfig{" +
                "peerMap=" + peerMap +
                ", serverId=" + serverId +
                ", leaderLeaseSeconds=" + leaderLeaseSeconds +
                '}';
    }
}