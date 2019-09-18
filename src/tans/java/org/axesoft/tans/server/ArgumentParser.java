package org.axesoft.tans.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.axesoft.jaxos.JaxosSettings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author gaoyuan
 * @sine 2019/8/26.
 */
public class ArgumentParser {
    public static class Args {
        @Parameter(names = {"-i"}, description = "Self id")
        private Integer id = 0;

        @Parameter(names = {"-f"}, description = "config file name")
        private String configFilename = "./config/jaxos.properties";

        @Parameter(names = {"-g"}, description = "ignore other leader, always do propose")
        private boolean ignoreLeader = false;

        @Parameter(names = {"-d"}, description = "Directory of DB")
        private String dbDirectory;

        @Parameter(names = {"-p"}, description = "Partition number")
        private Integer partitionNumber = 1;
    }

    private Properties properties;
    private Map<Integer, Integer> peerHttpMap;

    public ArgumentParser() {
    }

    /**
     * @param properties provided properties will take over the property file
     */
    public ArgumentParser(Properties properties) {
        this.properties = checkNotNull(properties);
    }

    public TansConfig parse(String[] sx){
        JaxosSettings config = parseJaxosConfig(sx);

        return new TansConfig(config, this.peerHttpMap);
    }

    public JaxosSettings parseJaxosConfig(String[] sx) {
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(sx);

        if (Strings.isNullOrEmpty(args.dbDirectory)) {
            throw new IllegalArgumentException("parameter \"-d\" dbDirectory not set");
        }

        JaxosSettings.Builder b = JaxosSettings.builder()
                .setServerId(args.id)
                .setDbDirectory(args.dbDirectory)
                .setPartitionNumber(args.partitionNumber)
                .setIgnoreLeader(args.ignoreLeader);

        if(this.properties == null) {
            this.properties = new Properties();
            try {
                properties.load(new BufferedReader(new FileReader(args.configFilename)));
            } catch (IOException e) {
                this.properties = null;
                throw new RuntimeException(e);
            }
        }

        loadAddressFromFile(b, this.properties, args.id);

        return b.build();
    }

    private void loadAddressFromFile(JaxosSettings.Builder builder, Properties properties, int selfId) {
        ImmutableMap.Builder<Integer, Integer> peerHttpMapBuilder = ImmutableMap.builder();

        boolean selfPortSet = false;
        for (String k : properties.stringPropertyNames()) {
            if (!k.startsWith("peer.")) {
                continue;
            }
            String[] sx = k.split("\\.");
            int id = Integer.parseInt(sx[1]);

            String[] ax = properties.getProperty(k).split(":");
            String address = ax[0];
            int port = Integer.parseInt(ax[1]);
            int httpPort = Integer.parseInt(ax[2]);

            JaxosSettings.Peer peer = new JaxosSettings.Peer(id, address, port);
            if (id == selfId) {
                if (selfPortSet) {
                    throw new IllegalArgumentException("more than one self");
                }
                builder.setSelf(peer);
                selfPortSet = true;
            }
            builder.addPeer(peer);

            peerHttpMapBuilder.put(id, httpPort);
        }

        if (!selfPortSet) {
            throw new IllegalArgumentException("self setting not found selfId=" + selfId);
        }

        this.peerHttpMap = peerHttpMapBuilder.build();
    }
}
