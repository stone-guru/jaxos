package org.axesoft.tans.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.axesoft.jaxos.JaxosSettings;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

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
        private Boolean ignoreLeader = null;

        @Parameter(names = {"-d"}, description = "Directory of DB")
        private String dbDirectory = null;

        @Parameter(names = {"-p"}, description = "Partition number")
        private Integer partitionNumber = 0;

        @Parameter(names = {"-b"}, description = "batch size for HTTP request")
        private Integer requestBatchSize = 8;

        @Parameter(names = {"-c"}, description = "Interval in minutes of saving checkpoint ")
        private Integer checkPointMinutes = null;
    }

    private Properties properties;
    private Map<Integer, Integer> peerHttpMap;

    private Map<String, ItemParser> configItemMap;

    private int printMetricsSeconds = 60;

    public ArgumentParser() {
        ItemParser[] items = new ItemParser[]{
                new PartitionNumberItemParser("partition.number", 1, 64),
                new CheckPointMinutesItemParser("checkPoint.minutes", 1, Integer.MAX_VALUE),
                new LeaderLeaseItemParser("leader.lease.seconds", 1, Integer.MAX_VALUE),
                new PrintMetricsSecondsItemParser("metrics.print.seconds", 1, Integer.MAX_VALUE),
                new IgnoreLeaderItemParser("leader.ignore"),
                new AlgoThreadItemParser("core.thread.number", 1, 32),
                new LearnTimeoutParser("core.learn.timeout"),
                new LoggerSyncIntervalParser("logger.sync.interval"),
                new PeerTimeoutParser("core.peer.timeout")
        };
        this.configItemMap = Arrays.stream(items).collect(Collectors.toMap(ItemParser::itemName, i -> i));
    }

    /**
     * @param properties provided properties will take over the property file
     */
    public ArgumentParser(Properties properties) {
        this();
        this.properties = checkNotNull(properties);
    }

    public TansConfig parse(String[] sx) {
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(sx);

        JaxosSettings config = buildJaxosConfig(args);

        return new TansConfig(config, this.peerHttpMap, args.requestBatchSize);
    }

    public JaxosSettings parseJaxosSettings(String[] sx) {
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
            }
            catch (IOException e) {
                this.properties = null;
                throw new RuntimeException(e);
            }
        }

        //first: load from file
        //second: command arguments may overwrite it
        JaxosSettings.Builder builder = loadConfigFromFile(this.properties, args.id)
                .setServerId(args.id)
                .setDbDirectory(args.dbDirectory);
                //.setCheckPointMinutes(args.checkPointMinutes);

        if (args.ignoreLeader != null) {
            builder.setLeaderOnly(args.ignoreLeader);
        }

        if (args.partitionNumber > 0) {
            builder.setPartitionNumber(args.partitionNumber);
        }

        if (args.checkPointMinutes != null){
            builder.setCheckPointMinutes(args.checkPointMinutes);
        }
        return builder.build();
    }

    private JaxosSettings.Builder loadConfigFromFile(Properties properties, int selfId) {
        JaxosSettings.Builder builder = JaxosSettings.builder();
        ImmutableMap.Builder<Integer, Integer> peerHttpMapBuilder = ImmutableMap.builder();

        boolean selfPortSet = false;
        for (String k : properties.stringPropertyNames()) {
            String v = properties.getProperty(k);

            if (k.startsWith("peer.")) {
                String[] sx = k.split("\\.");
                int id = Integer.parseInt(sx[1]);

                String[] ax = v.split(":");
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
            } else {
                ItemParser parser = this.configItemMap.get(k);
                if(parser != null){
                    parser.parse(v, builder);
                } else {
                    System.err.println("Ignore unknown config item '" + k + "'");
                }
            }
        }

        if (!selfPortSet) {
            throw new IllegalArgumentException("self setting not found selfId=" + selfId);
        }

        this.peerHttpMap = peerHttpMapBuilder.build();
        return builder;
    }

    private static abstract class ItemParser {
        private String itemName;

        public ItemParser(String itemName) {
            this.itemName = itemName;
        }

        public String itemName() {
            return itemName;
        }

        abstract void parse(String value, JaxosSettings.Builder builder);
    }

    private static abstract class DurationItemParser extends ItemParser {
        private static Map<Character, Integer> unitMap = ImmutableMap.of(
                'm', 1, //milli second
                's', 1000, //second
                'M', 60 * 1000, //minute
                'h', 60 * 60 * 1000);//hour


        public DurationItemParser(String itemName) {
            super(itemName);
        }

        @Override
        void parse(String value, JaxosSettings.Builder builder) {
            if(value == null || value.length() < 2){
                throw new IllegalArgumentException("'" + value + "' is not a valid time duration");
            }
            char u = value.charAt(value.length() - 1);
            if(!unitMap.containsKey(u)){
                throw new IllegalArgumentException("Unknown duration unit " + u);
            }

            String s = value.substring(0, value.length() - 1);
            int v;
            try {
                v = Integer.parseInt(s);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("value " + s + " of " + itemName() + " is not valid duration num");
            }

            this.accept(Duration.ofMillis(v * unitMap.get(u)), builder);
        }

        abstract void accept(Duration d, JaxosSettings.Builder builder);
    }

    private static class LearnTimeoutParser extends DurationItemParser {
        public LearnTimeoutParser(String itemName) {
            super(itemName);
        }

        @Override
        void accept(Duration d, JaxosSettings.Builder builder) {
            builder.setLearnTimeout(d);
        }
    }

    private static class LoggerSyncIntervalParser extends DurationItemParser {
        public LoggerSyncIntervalParser(String itemName) {
            super(itemName);
        }

        @Override
        void accept(Duration d, JaxosSettings.Builder builder) {
            builder.setSyncInterval(d);
        }
    }

    private static class PeerTimeoutParser extends DurationItemParser {
        public PeerTimeoutParser(String itemName) {
            super(itemName);
        }

        @Override
        void accept(Duration d, JaxosSettings.Builder builder) {
            builder.setPrepareTimeoutMillis(d.toMillis());
            builder.setAcceptTimeoutMillis(d.toMillis());
        }
    }


    private static abstract class IntItemParser extends ItemParser {
        private int low;
        private int high;
        private boolean parsed;

        public IntItemParser(String itemName, int low, int high) {
            super(itemName);
            this.low = low;
            this.high = high;
        }

        @Override
        void parse(String value, JaxosSettings.Builder builder) {
            if(this.parsed){
                throw new IllegalArgumentException("'" + itemName() + "' duplicated");
            }
            int x = 0;
            try {
                x = Integer.parseInt(value);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("value " + value + " of " + itemName() + " is not valid integer");
            }
            if (x > high) {
                throw new IllegalArgumentException("value " + value + " of " + itemName() + " large than " + this.high);
            }
            if (x < low) {
                throw new IllegalArgumentException("value " + value + " of " + itemName() + " less than " + this.low);
            }
            accept(x, builder);
            this.parsed = true;
        }

        public boolean parsed() {
            return this.parsed;
        }

        abstract void accept(int v, JaxosSettings.Builder builder);
    }

    private static abstract class BoolItemParser extends ItemParser {
        public BoolItemParser(String itemName) {
            super(itemName);
        }

        @Override
        void parse(String value, JaxosSettings.Builder builder) {
            boolean b;
            if ("true".equals(value)) {
                b = true;
            }
            else if ("false".equals(value)) {
                b = false;
            }
            else {
                throw new IllegalArgumentException("Unknown boolean value '" + value + "' for  '" + itemName() + "'");
            }
            accept(b, builder);

        }

        abstract void accept(boolean b, JaxosSettings.Builder builder);
    }

    private static class PartitionNumberItemParser extends IntItemParser {
        public PartitionNumberItemParser(String itemName, int low, int high) {
            super(itemName, low, high);
        }

        @Override
        void accept(int v, JaxosSettings.Builder builder) {
            builder.setPartitionNumber(v);
        }
    }

    private static class IgnoreLeaderItemParser extends BoolItemParser {
        public IgnoreLeaderItemParser(String itemName) {
            super(itemName);
        }

        @Override
        void accept(boolean b, JaxosSettings.Builder builder) {
            if(b){
                builder.setLeaderOnly(false);
            }
        }
    }

    private static class CheckPointMinutesItemParser extends IntItemParser {
        public CheckPointMinutesItemParser(String itemName, int low, int high) {
            super(itemName, low, high);
        }

        @Override
        void accept(int v, JaxosSettings.Builder builder) {
            builder.setCheckPointMinutes(v);
        }
    }

    private  class PrintMetricsSecondsItemParser extends IntItemParser {

        public PrintMetricsSecondsItemParser(String itemName, int low, int high) {
            super(itemName, low, high);
        }

        @Override
        void accept(int v, JaxosSettings.Builder builder) {
            ArgumentParser.this.printMetricsSeconds = v;
        }
    }

    private static class LeaderLeaseItemParser extends IntItemParser {
        public LeaderLeaseItemParser(String itemName, int low, int high) {
            super(itemName, low, high);
        }

        @Override
        void accept(int v, JaxosSettings.Builder builder) {
            builder.setLeaderLeaseSeconds(v);
        }
    }

    private static class AlgoThreadItemParser extends IntItemParser {
        public AlgoThreadItemParser(String itemName, int low, int high) {
            super(itemName, low, high);
        }

        @Override
        void accept(int v, JaxosSettings.Builder builder) {
            builder.setAlgoThreadNumber(v);
        }
    }
}
