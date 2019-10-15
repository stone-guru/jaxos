package org.axesoft.jaxos;

import com.google.protobuf.ByteString;
import org.apache.commons.lang3.time.StopWatch;
import org.axesoft.jaxos.algo.AcceptorLogger;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.InstanceValue;
import org.axesoft.jaxos.logger.LevelDbAcceptorLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;


public class AcceptorLoggerTest {
    final static String DB_DIR = "/home/bison/tmp/jaxosdb";
    static AcceptorLogger logger;

    @BeforeClass
    public static void setup(){
        File dir = new File(DB_DIR);
        if(!dir.exists()){
            dir.mkdir();
        }
        logger = new LevelDbAcceptorLogger(DB_DIR, Duration.ofMillis(20));
    }

    @AfterClass
    public static void shutdown(){
        logger.close();
    }

    @Test
    public void testSave(){
        final int n = 10000;
        StopWatch w = StopWatch.createStarted();
        for(int i = 0; i < n; i++) {
            logger.savePromise(1, 1000 + i, 1 + i, Event.BallotValue.appValue(ByteString.copyFromUtf8("Hello" + i)));
            logger.savePromise(2, 1000 + i, 1 + i, Event.BallotValue.appValue(ByteString.copyFromUtf8("Hello" + i)));
        }

        w.stop();
        double seconds = w.getTime(TimeUnit.MILLISECONDS)/1000.0;
        System.out.println(String.format("Insert %d records in %.2f s, OPS = %.2f", 2 * n, seconds, (2.0 * n)/seconds));

        InstanceValue p = logger.loadLastPromise(1);
        assertNotNull(p);

        assertEquals(1, p.squadId);
        assertEquals(1000 + n - 1, p.instanceId);
        assertEquals(n, p.proposal);
        assertEquals("Hello" + (n - 1), p.value.content().toStringUtf8());
    }
}
