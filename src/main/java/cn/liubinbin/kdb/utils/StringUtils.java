package cn.liubinbin.kdb.utils;


/**
 * @author liubinbin
 * @date 2024/08/16
 */
public class StringUtils {

    public static String repeat(String s, int count) {
        if (count <= 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(s.length() * count);
        for (int i = 0; i < count; i++) {
            builder.append(s);
        }
        return builder.toString();
    }

    public static String rightPaddingToFixLen(String original, int length){
        return String.format("%-" + length + "s", original);
    }

}
