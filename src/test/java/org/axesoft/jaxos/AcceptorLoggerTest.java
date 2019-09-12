package org.axesoft.jaxos;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.logger.BerkeleyDbAcceptorLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


public class AcceptorLoggerTest {
    static AcceptorLogger logger;

    @BeforeClass
    public static void setup(){
        logger = new BerkeleyDbAcceptorLogger("/tmp");
    }

    @AfterClass
    public static void shutdown(){
        logger.close();
    }

    @Test
    public void testSave(){
        logger.saveLastPromise(1, 1000, 9, ByteString.copyFromUtf8("Hello"));
        AcceptorLogger.Promise p = logger.loadLastPromise(1);
        assertNotNull(p);

        assertEquals(1, p.squadId);
        assertEquals(1000, p.instanceId);
        assertEquals(9, p.proposal);
        assertEquals("Hello", p.value.toStringUtf8());
    }
}
