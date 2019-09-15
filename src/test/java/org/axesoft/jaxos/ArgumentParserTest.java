package org.axesoft.jaxos;

import org.apache.commons.lang3.ArrayUtils;
import org.axesoft.jaxos.app.ArgumentParser;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author gaoyuan
 * @sine 2019/8/26.
 */
public class ArgumentParserTest {
    @Test
    public void testParse1() throws Exception {
        Properties properties = new Properties();
        properties.put("peer.1", "127.0.0.1:110:119");
        properties.put("peer.2", "127.0.0.1:120:129");
        properties.put("peer.3", "127.0.0.1:130:139");

        String[] args = new String[]{"-i", "1", "-g", "-d", "./"};
        JaxosConfig config = new ArgumentParser(properties).parse(args);

        assertEquals(1, config.serverId());
        assertEquals(110, config.self().port());
        assertEquals(119, config.self().httpPort());
        assertEquals("./", config.dbDirectory());
        assertTrue(config.ignoreLeader());

        assertNotNull(config.getPeer(2));
        assertNotNull(config.getPeer(3));
        assertEquals(3, config.peerCount());
    }
}
