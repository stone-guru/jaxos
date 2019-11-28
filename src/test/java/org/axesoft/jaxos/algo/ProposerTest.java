package org.axesoft.jaxos.algo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author bison
 * @sine 2019/11/28.
 */
public class ProposerTest {
    @Test
    public void testBallotId1() throws Exception {
        Proposer.BallotIdHolder holder = new Proposer.BallotIdHolder(7);
        long i1 = holder.nextId();
        //System.out.println(String.format("%X", i1));

        long i2 = holder.nextId();
        //System.out.println(String.format("%X", i2));

        assertEquals(i2, i1 + 1);
    }

    @Test
    public void testBallotId2() throws Exception {
        Proposer.BallotIdHolder holder = new Proposer.BallotIdHolder(7);
        for(int i = 0;i < Integer.MAX_VALUE; i++){
            holder.nextId();
        }
        long i1 = holder.nextId();
        //System.out.println(String.format("%X", i1));

        for(int i = 0;i < Integer.MAX_VALUE; i++){
            holder.nextId();
        }
        long i2 = holder.nextId();
        //System.out.println(String.format("%X", i2));

        long i3 = holder.nextId();
        //System.out.println(String.format("%X", i3));
        assertEquals(7L << 32, i3);
    }
}
