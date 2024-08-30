package cn.liubinbin.kdb.utils;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * @author liubinbin
 * @date 2024/08/30
 */
public class ByteUtilsTest {

    @Test
    public void testSetBit() {
        Integer res = ByteUtils.setBit(0,1, 1);
        assertEquals(0, res - 2);
        res = ByteUtils.setBit(0,0, 1);
        assertEquals(0, res - 1);
    }

    @Test
    public void testGetBitAsBool() {
        assertTrue(ByteUtils.getBitAsBool(1, 0));
        assertFalse(ByteUtils.getBitAsBool(0, 0));
    }
}
