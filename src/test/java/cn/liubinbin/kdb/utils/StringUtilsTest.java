package cn.liubinbin.kdb.utils;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author liubinbin
 * @date 2024/08/30
 */
public class StringUtilsTest {

    @Test
    public void repeatShouldRight() {
        String s = "a";
        int count = 5;
        String expected = "aaaaa";
        String actual = StringUtils.repeat(s, count);
        assertEquals(expected, actual);
    }

    @Test
    public void rightPaddingToFixLenShouldRight() {
        String original = "a";
        int length = 5;
        String expected = "a    ";
        String actual = StringUtils.rightPaddingToFixLen(original, length);
        System.out.println(actual.length());
        System.out.println(expected.length());
        assertEquals(expected, actual);
    }
}
