package org.axesoft.tans.server;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.tuple.Pair;
import org.axesoft.jaxos.JaxosSettings;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SettingsParser {
    private Map<String, Object> yamlRoot;
    private JaxosSettings.Builder builder;
    private ImmutableMap.Builder<Integer, Integer> peerHttpPortMapBuilder = ImmutableMap.builder();
    private Map<String, ItemParser> itemParserMap;
    private int requestBatchSize = -1;

    public SettingsParser(){
        builder = JaxosSettings.builder();
        ItemParser[] items = new ItemParser[]{
                new IntItemParser("core.id", i -> builder.setServerId(i), 1, 32),
                new IntItemParser("core.partition.number", i -> builder.setPartitionNumber(i), 1, 64),
                new DurationItemParser("core.peer.timeout", d -> {
                    builder.setAcceptTimeoutMillis(d.toMillis());
                    builder.setPrepareTimeoutMillis(d.toMillis());
                }),
                new IntItemParser("core.thread.number", i -> builder.setAlgoThreadNumber(i), 1, 32),

                new DurationItemParser("leader.lease", d -> builder.setLeaderLeaseSeconds((int)d.toSeconds())),
                new BoolItemParser("leader.mandatory", b -> builder.setLeaderOnly(b)),

                new IntItemParser("db.checkpoint.minutes", i -> builder.setCheckPointMinutes(i)),
                new StringItemParser("db.path", s -> builder.setDbDirectory(s)),
                new DurationItemParser("db.sync", d -> builder.setSyncInterval(d)),

                new DurationItemParser("learn.timeout", d -> builder.setLearnTimeout(d)),
                new IntItemParser("learn.max-instance", i -> builder.setLearnInstanceLimit(i)),
                new IntItemParser("learn.max-send", i -> builder.setSendInstanceLimit(i)),
                new IntItemParser("tans.batch-size", i -> this.requestBatchSize = i, 1, 512),
                new PeerListParser("peers")
        };

        this.itemParserMap = Arrays.stream(items).collect(Collectors.toMap(ItemParser::itemName, i -> i));
    }

    public Optional<TansConfig> parseArgs(String[] args){
        Options options = new Options();

        options.addOption("c", "config-file", true, "config file");
        for(String s : argNames()){
            if(!"peers".equals(s)) {
                options.addOption(null, s, true, s);
            }
        }

        CommandLineParser cliParser = new DefaultParser();
        CommandLine cli = null;
        try {
            cli = cliParser.parse(options, args);
        }
        catch (ParseException e) {
            e.printStackTrace(System.err);
            return Optional.empty();
        }

        String configFile = cli.getOptionValue('c', "./config/settings.yml");

        InputStream is;
        try {
            is = new FileInputStream(configFile);
        }
        catch (FileNotFoundException e) {
            System.err.println("File not found: " + configFile);
            return Optional.empty();

        }

        parse(is);

        for(String s : argNames()){
            if(cli.hasOption(s)){
                parseString(s, cli.getOptionValue(s));
            }
        }

        return Optional.of(new TansConfig(builder.build(), peerHttpPortMap(), requestBatchSize()));
    }


    public JaxosSettings.Builder parse(InputStream is){
        yamlRoot = (Map<String, Object>)new Yaml().load(is);


        parseSegment(null, this.yamlRoot);


        return this.builder;
    }

    public void parseString(String argName, String argValue){
        ItemParser p = this.itemParserMap.get(argName);
        if(p == null){
            throw new IllegalArgumentException("Unknown arg " + argName);
        }
        p.parseString(argValue);
    }

    public Set<String> argNames(){
        return this.itemParserMap.keySet();
    }

    private void parseSegment(String prefix, Map<String, Object> valueMap){
        for(String key : valueMap.keySet()){
            Object v = valueMap.get(key);
            String fullKey = appendPrefixMaybe(prefix, key);
            if(v instanceof  Map<?, ?>){
                parseSegment(fullKey, (Map<String, Object>)v);
            } else if (v != null){
                ItemParser p = this.itemParserMap.get(fullKey);
                if(p == null){
                    System.err.println("ignore unknown item " + fullKey);
                } else {
                    p.parseObject(v);
                }
            } else {
                System.err.println("ignore empty item " + fullKey);
            }
        }
    }

    private String appendPrefixMaybe(String prefix, String s){
        return prefix == null ? s : prefix + "." + s;
    }

    public JaxosSettings.Builder jaxosSettingsBuilder(){
        return this.builder;
    }

    public Map<Integer, Integer> peerHttpPortMap(){
        return this.peerHttpPortMapBuilder.build();
    }


    public int requestBatchSize(){
        return this.requestBatchSize;
    }

    private static abstract class ItemParser {
        private String itemName;

        public ItemParser(String itemName) {
            this.itemName = itemName;
        }

        public String itemName() {
            return itemName;
        }

        abstract void parseObject(Object value);

        void parseString(String s) {

        }
    }

    private class PeerListParser extends ItemParser {

        public PeerListParser(String itemName) {
            super(itemName);
        }

        @Override
        void parseObject(Object value) {
            List<Map<String, Object>> peers = (List<Map<String, Object>>)value;
            for(Map<String, Object> kv : peers){
                Pair<JaxosSettings.Peer,Integer> p = parsePeer(kv);
                builder.addPeer(p.getLeft());
                peerHttpPortMapBuilder.put(p.getLeft().id(), p.getRight());
            }
        }


        private Pair<JaxosSettings.Peer,Integer> parsePeer(Map<String, Object> kv){
            return Pair.of(new JaxosSettings.Peer((int)kv.get("id"), (String)kv.get("hostname"), (int)kv.get("consensus-port")),
                    (int)kv.get("http-port"));
        }
    }


    private static class BoolItemParser extends ItemParser {
        private Consumer<Boolean> consumer;

        public BoolItemParser(String itemName, Consumer<Boolean> consumer) {
            super(itemName);
            this.consumer = consumer;
        }

        @Override
        void parseObject(Object value) {
            this.consumer.accept((boolean)value);
        }

        @Override
        void parseString(String s) {
            if("true".equals(s)){
                this.consumer.accept(true);
            } else if ("false".equals(s)){
                this.consumer.accept(false);
            } else {
                throw new RuntimeException("Unknown bool value :" + s);
            }
        }
    }

    private static class StringItemParser extends ItemParser{
        private Consumer<String> consumer;
        public StringItemParser(String itemName, Consumer<String> consumer) {
            super(itemName);
            this.consumer = consumer;
        }

        @Override
        void parseObject(Object value){
            consumer.accept((String)value);
        }

        @Override
        void parseString(String s) {
            consumer.accept(s);
        }
    }


    private static class DurationItemParser extends ItemParser {
        private static Map<Character, Integer> unitMap = ImmutableMap.of(
                'm', 1, //milli second
                's', 1000, //second
                'M', 60 * 1000, //minute
                'h', 60 * 60 * 1000);//hour

        private Consumer<Duration> consumer;
        public DurationItemParser(String itemName,  Consumer<Duration> consumer) {
            super(itemName);
            this.consumer = consumer;
        }

        @Override
        void parseObject(Object value) {
            String s = (String)value;
            if(s == null || s.length() < 2){
                throw new IllegalArgumentException("'" + s + "' is not a valid time duration");
            }
            char u = s.charAt(s.length() - 1);
            if(!unitMap.containsKey(u)){
                throw new IllegalArgumentException("Unknown duration unit " + u);
            }

            String sd = s.substring(0, s.length() - 1);
            int v;
            try {
                v = Integer.parseInt(sd);
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException("value " + s + " of " + itemName() + " is not valid duration num");
            }

            consumer.accept(Duration.ofMillis(v * unitMap.get(u)));
        }

        @Override
        void parseString(String s) {
            this.parseObject(s);
        }
    }

    private static class IntItemParser extends ItemParser {
        private int low;
        private int high;
        private boolean parsed;
        private Consumer<Integer> consumer;
        public IntItemParser(String itemName, Consumer<Integer> consumer, int low, int high) {
            super(itemName);
            this.consumer = consumer;
            this.low = low;
            this.high = high;
        }

        public IntItemParser(String itemName, Consumer<Integer> consumer) {
            this(itemName, consumer, Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        @Override
        void parseObject(Object value) {
            int x = (int)value;
            if(this.parsed){
                throw new IllegalArgumentException("'" + itemName() + "' duplicated");
            }
            if (x > high) {
                throw new IllegalArgumentException("value " + x + " of " + itemName() + " large than " + this.high);
            }
            if (x < low) {
                throw new IllegalArgumentException("value " + x + " of " + itemName() + " less than " + this.low);
            }
            this.consumer.accept(x);
            this.parsed = true;
        }

        public boolean parsed() {
            return this.parsed;
        }

        @Override
        void parseString(String s) {
            this.parseObject(Integer.parseInt(s));
        }
    }
}
