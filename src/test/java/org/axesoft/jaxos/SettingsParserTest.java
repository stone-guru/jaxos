package org.axesoft.jaxos;

import com.google.common.collect.ImmutableMap;
import org.axesoft.tans.server.SettingsParser;
import org.axesoft.tans.server.TansConfig;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class SettingsParserTest {
    @Test
    public void test1(){
        JaxosSettings settings = parseWithExtraArgs(ImmutableMap.of("db.path", "/var/data"));
        assertEquals("/var/data", settings.dbDirectory());
    }

    private JaxosSettings parseWithExtraArgs(Map<String, String> args){
        List<String> sx = new ArrayList<>();
        sx.add("-c");
        sx.add("./src/tans-test/resources/settings-test.yml");
        for(Map.Entry<String, String> entry : args.entrySet()){
            sx.add("--" + entry.getKey());
            sx.add(entry.getValue());
        }

        String[] argx = sx.toArray(new String[0]);
        SettingsParser   parser = new SettingsParser();
        Optional<TansConfig> opt = parser.parseArgs(argx);

        if(opt.isPresent()){
            System.out.println(opt.get().jaxConfig());
            return opt.get().jaxConfig();
        } else {
            throw new RuntimeException("parse error");
        }
    }
}
