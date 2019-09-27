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
        private Integer partitionNumber = 0;

        @Parameter(names = {"-m"}, description = "Interval in seconds of print metrics")
        private Integer printMetricsInterval = 10;

        @Parameter(names = {"-b"}, description = "batch size for HTTP request")
        private Integer requestBatchSize = 8;

        @Parameter(names = {"-c"}, description = "Minutes interval of making checkpoint ")
        private Integer checkPointMinutes = 1;
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

    public TansConfig parse(String[] sx) {
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(sx);

        JaxosSettings config = buildJaxosConfig(args);

        return new TansConfig(config, this.peerHttpMap, args.printMetricsInterval, args.requestBatchSize);
    }

    public JaxosSettings parseJaxosSettings(String[] sx){
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(sx);

        return buildJaxosConfig(args);
    }

    private JaxosSettings buildJaxosConfig(Args args) {
        if (Strings.isNullOrEmpty(args.dbDirectory)) {
            throw new IllegalArgumentException("parameter \"-d\" dbDirectory not set");
        }


        if (this.properties == null) {
            this.properties = new Properties();
            try {
                properties.load(new BufferedReader(new FileReader(args.configFilename)));
            } catch (IOException e) {
                this.properties = null;
                throw new RuntimeException(e);
            }
        }

        //first: load from file
        //second: command arguments may overwrite it
        JaxosSettings.Builder builder = loadConfigFromFile(this.properties, args.id)
                .setServerId(args.id)
                .setDbDirectory(args.dbDirectory)
                .setCheckPointMinutes(args.checkPointMinutes)
                .setIgnoreLeader(args.ignoreLeader);

        if (args.partitionNumber > 0) {
            builder.setPartitionNumber(args.partitionNumber);
        }

        return builder.build();
    }

    private JaxosSettings.Builder loadConfigFromFile(Properties properties, int selfId) {
        JaxosSettings.Builder builder = JaxosSettings.builder();
        ImmutableMap.Builder<Integer, Integer> peerHttpMapBuilder = ImmutableMap.builder();

        boolean selfPortSet = false;
        for (String k : properties.stringPropertyNames()) {
            String s = properties.getProperty(k);

            if (k.startsWith("peer.")) {
                String[] sx = k.split("\\.");
                int id = Integer.parseInt(sx[1]);

                String[] ax = s.split(":");
                String address = ax[0];
                int port = Integer.parseInt(ax[1]);
                int httpPort = Integer.parseInt(ax[2]);

                JaxosSettings.Peer peer = new JaxosSettings.Peer(id, address, port);
                if (id == selfId) {
                    if (selfPortSet) {
                        throw new IllegalArgumentException("more than one self");
                    }
                    selfPortSet = true;
                }
                builder.addPeer(peer);

                peerHttpMapBuilder.put(id, httpPort);
            }
            else if (k.equals("partition.number")) {
                int n = Integer.parseInt(s);
                if (n < 0 || n > 32) {
                    throw new IllegalArgumentException("Partition number should be in [1, 32], actual is " + n);
                }
                builder.setPartitionNumber(n);
            }
            else if (k.equals("checkPoint.minutes")){
                builder.setCheckPointMinutes(Integer.parseInt(s));
            }
        }

        if (!selfPortSet) {
            throw new IllegalArgumentException("self setting not found selfId=" + selfId);
        }

        this.peerHttpMap = peerHttpMapBuilder.build();
        return builder;
    }
}
