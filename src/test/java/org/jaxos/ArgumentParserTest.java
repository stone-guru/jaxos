package org.jaxos;

import org.jaxos.app.ArgumentParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author gaoyuan
 * @sine 2019/8/26.
 */
public class ArgumentParserTest {
    @Test
    public void testParse() throws Exception {
        String[] args = new String[]{"-i", "2", "-r", "0:a.b.c.e:9990", "-r", "1:1.1.1.1:8000", "-r", "2:1.1.1.2:9000"};
        JaxosConfig config = new ArgumentParser().parse(args);
        assertEquals(2, config.serverId());
        assertEquals(9000, config.port());
        assertEquals(2, config.peerMap().size());
        assertEquals(9990, config.getPeer(0).port());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoSelf() throws Exception {
        String[] args = new String[]{"-i", "3", "-r", "0:a.b.c.e:9990", "-r", "1:1.1.1.1:8000", "-r", "2:1.1.1.2:9000"};
        new ArgumentParser().parse(args);
    }
}
