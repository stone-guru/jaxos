package org.jaxos.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.jaxos.JaxosConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author gaoyuan
 * @sine 2019/8/26.
 */
public class ArgumentParser {
    public static class Args {
        @Parameter
        private List<String> parameters = new ArrayList<>();

        @Parameter(names = {"-i"}, description = "Self id")
        private Integer id = 0;

        @Parameter(names = {"-f"}, description = "config file name")
        private String configFilename;

        @Parameter(names = {"-p"}, description = "HTTP server port")
        private Integer httpPort = 0;

    }

    public JaxosConfig parse(String[] sx) {
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(sx);

        JaxosConfig.Builder b = JaxosConfig.builder()
                .setServerId(args.id)
                .setHttpPort(args.httpPort);

        loadAddressFromFile(b, args.configFilename, args.id);

        return b.build();
    }

    private JaxosConfig.Builder loadAddressFromFile(JaxosConfig.Builder builder, String fileName, int selfId){
        Properties properties = new Properties();
        try {
            properties.load(new BufferedReader(new FileReader(fileName)));
        }
        catch (IOException e) {
            return builder;
        }
        boolean selfPortSet = false;
        for(String k : properties.stringPropertyNames()){
            if(!k.startsWith("peer.")){
                continue;
            }
            String[] sx = k.split("\\.");
            int id = Integer.parseInt(sx[1]);

            String[] ax = properties.getProperty(k).split(":");
            String address = ax[0];
            int port = Integer.parseInt(ax[1]);

            if (id == selfId) {
                if(selfPortSet){
                    throw new IllegalArgumentException("more than one self");
                }
                builder.setPort(port);
                selfPortSet = true;
            } else {
                builder.addPeer(id, address, port);
            }
        }

        if(!selfPortSet){
            throw new IllegalArgumentException("self setting not found selfId=" + selfId);
        }

        return builder;
    }
}
