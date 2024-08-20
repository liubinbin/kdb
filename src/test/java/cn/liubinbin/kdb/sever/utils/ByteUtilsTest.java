package cn.liubinbin.kdb.sever.utils;

import cn.liubinbin.kdb.utils.ByteUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteUtilsTest {

    @Test
    public void testSetBit() {
        Integer res = ByteUtils.setBit(0,1, 1);
        assertEquals(0, res - 2);
        res = ByteUtils.setBit(0,0, 1);
        assertEquals(0, res - 1);
    }
}
