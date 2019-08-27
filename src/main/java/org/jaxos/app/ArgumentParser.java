package org.jaxos.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.jaxos.JaxosConfig;

import java.util.ArrayList;
import java.util.List;

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

        @Parameter(names = "-r", description = "All Peers")
        private List<String> peers = new ArrayList<>();
    }

    public JaxosConfig parse(String[] sx) {
        Args args = new Args();

        JCommander.newBuilder()
                .addObject(args)
                .build()
                .parse(sx);

        JaxosConfig.Builder b = JaxosConfig.builder();
        b.setServerId(args.id);

        boolean selfPortSet = false;
        for (String peer : args.peers) {
            String[] px = peer.split(":");
            if (px.length != 3) {
                throw new IllegalArgumentException(peer);
            }
            int id = Integer.parseInt(px[0]);
            String address = px[1];
            int port = Integer.parseInt(px[2]);

            if (id == args.id.intValue()) {
                if(selfPortSet){
                    throw new IllegalArgumentException("more than one self");
                }
                b.setPort(port);
                selfPortSet = true;
            }
            else {
                b.addPeer(id, address, port);
            }
        }

        if(!selfPortSet){
            throw new IllegalArgumentException("self setting not found selfId=" + args.id);
        }

        return b.build();
    }
}
