package org.axesoft.jaxos;

import org.apache.commons.lang3.ArrayUtils;
import org.axesoft.jaxos.app.ArgumentParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author gaoyuan
 * @sine 2019/8/26.
 */
public class ArgumentParserTest {
    @Test
    public void testParse1() throws Exception {
        String[] args = new String[]{"-i", "1", "-g"};
        JaxosConfig config = new ArgumentParser().parse(args);
        assertTrue(config.ignoreLeader());
    }

    @Test
    public void testParse2() throws Exception {
        String[] args = new String[]{"-i", "1"};
        JaxosConfig config = new ArgumentParser().parse(args);
        assertFalse(config.ignoreLeader());
    }
}
