package org.axesoft.jaxos;

import org.axesoft.tans.server.ArgumentParser;
import org.junit.Test;

import java.time.Duration;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author gaoyuan
 * @sine 2019/8/26.
 */
public class ArgumentParserTest {
    @Test
    public void testParse1() throws Exception {
        System.out.println(Integer.MAX_VALUE + "," + Long.MAX_VALUE);
        Properties properties = new Properties();
        properties.put("peer.1", "127.0.0.1:110:119");
        properties.put("peer.2", "127.0.0.1:120:129");
        properties.put("peer.3", "127.0.0.1:130:139");

        properties.put("core.learn.timeout", "2s");
        properties.put("logger.sync.interval", "5s");
        properties.put("core.peer.timeout", "300m");
        String[] args = new String[]{"-i", "1", "-g", "-d", "./"};
        JaxosSettings config = new ArgumentParser(properties).parseJaxosSettings(args);

        assertEquals(1, config.serverId());
        assertEquals(110, config.self().port());
        assertEquals("./", config.dbDirectory());
        assertTrue(config.leaderOnly());

        assertNotNull(config.getPeer(2));
        assertNotNull(config.getPeer(3));
        assertEquals(3, config.peerCount());

        assertEquals(Duration.ofSeconds(2), config.learnTimeout());

        assertEquals(Duration.ofSeconds(5), config.syncInterval());
        assertEquals(300, config.acceptTimeoutMillis());
        assertEquals(300, config.prepareTimeoutMillis());
    }
}
