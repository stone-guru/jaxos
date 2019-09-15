package org.axesoft.tans.server;

import com.google.common.collect.ImmutableMap;
import org.axesoft.jaxos.JaxosSettings;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TansConfig {
    private JaxosSettings jaxosSettings;
    private Map<Integer, Integer> peerHttpPorts;

    public TansConfig(JaxosSettings jaxosSettings, Map<Integer, Integer> peerHttpPorts) {
        this.jaxosSettings = checkNotNull(jaxosSettings);
        this.peerHttpPorts = checkNotNull(ImmutableMap.copyOf(peerHttpPorts));
    }

    public int serverId(){
        return jaxosSettings.serverId();
    }

    public String address(){
        return jaxosSettings.self().address();
    }

    public int httpPort(){
        return getPeerHttpPort(jaxosSettings.serverId());
    }

    public JaxosSettings jaxConfig(){
        return this.jaxosSettings;
    }

    public int getPeerHttpPort(int peerId){
        return peerHttpPorts.getOrDefault(peerId, 0);
    }
}
