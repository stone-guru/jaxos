package org.axesoft.tans.domain;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CounterTest {
    @Test
    public void test1(){
        String[] keys = new String[]{"pig.id","star.id", "girl.id", "monkey.id", "actress.id", "object-id-1"};
        System.out.println(Math.abs(Integer.MIN_VALUE));
        for(String k : keys){
            int c1 = k.hashCode();
            int c2 = (c1 < 0? -c1 : c1);
            System.out.println(String.format("key = %s, hashcode=%d, squad=%d", k, c2, c2%3));
        }
        assertTrue(1 == 1);
    }

    @Test
    public void testMask() throws Exception {

    }
}
