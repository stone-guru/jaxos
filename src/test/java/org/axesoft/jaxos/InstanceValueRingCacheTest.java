package org.axesoft.jaxos;

import com.google.protobuf.ByteString;
import io.netty.util.CharsetUtil;
import org.axesoft.jaxos.algo.Event;
import org.axesoft.jaxos.algo.InstanceValue;
import org.axesoft.jaxos.logger.InstanceValueRingCache;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author gaoyuan
 * @sine 2019/10/15.
 */
public class InstanceValueRingCacheTest {
    private InstanceValue newValue(long instanceId){
        return new InstanceValue(1, instanceId, 2,
                Event.BallotValue.appValue(ByteString.copyFrom(Long.toString(instanceId), CharsetUtil.UTF_8)));
    }

    @Test
    public void testSize(){
        InstanceValueRingCache cache = new InstanceValueRingCache(256);

        assertEquals(0, cache.size());
    }
}
