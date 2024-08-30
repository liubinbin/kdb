package cn.liubinbin.kdb.utils;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author liubinbin
 * @date 2024/8/30
 */
public class ByteArrayUtilsTest {

    @Test
    public void integerPutGetShouldWork() {
        int num = 123456;
        byte[] byteArray = new byte[4];
        ByteArrayUtils.putInt(byteArray, 0, num);
        int val = ByteArrayUtils.toInt(byteArray, 0);
        assertEquals(num, val);
    }

    @Test
    public void bytePutGetShouldWork() {
        byte num = 5;
        byte[] byteArray = new byte[3];
        ByteArrayUtils.putByte(byteArray, 0, num);
        byte val = ByteArrayUtils.toByte(byteArray, 0);
        assertEquals(num, val);
    }

}
