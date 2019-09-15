package org.axesoft.jaxos;

import com.google.protobuf.ByteString;
import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.logger.BerkeleyDbAcceptorLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


public class AcceptorLoggerTest {
    final static String DB_DIR = "/mnt/ram/jaxosdb";
    static AcceptorLogger logger;

    @BeforeClass
    public static void setup(){
        File dir = new File(DB_DIR);
        if(!dir.exists()){
            dir.mkdir();
        }
        logger = new BerkeleyDbAcceptorLogger(DB_DIR);
    }

    @AfterClass
    public static void shutdown(){
        logger.close();
    }

    @Test
    public void testSave(){
        for(int i = 0; i < 100; i++) {
            logger.savePromise(1, 1000 + i, 1 + i, ByteString.copyFromUtf8("Hello" + i));
        }

        AcceptorLogger.Promise p = logger.loadLastPromise(1);
        assertNotNull(p);

        assertEquals(1, p.squadId);
        assertEquals(1099, p.instanceId);
        assertEquals(100, p.proposal);
        assertEquals("Hello99", p.value.toStringUtf8());
    }
}
