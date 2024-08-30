package cn.liubinbin.kdb.sever.entity;


import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author liubinbin
 * @date 2024/8/30
 */
public class KdbRowValueTest {

    @Test
    public void compareToShouldRight() {
        KdbRowValue kdbRowValue1 = new KdbRowValue(ColumnType.INTEGER, 1);
        KdbRowValue kdbRowValue2 = new KdbRowValue(ColumnType.INTEGER, 2);
        KdbRowValue kdbRowValue3 = new KdbRowValue(ColumnType.INTEGER, 2);

        assertEquals(1, kdbRowValue2.compareTo(kdbRowValue1));
        assertEquals(-1, kdbRowValue1.compareTo(kdbRowValue2));
        assertEquals(0, kdbRowValue2.compareTo(kdbRowValue3));
    }

    @Test
    public void getStringValueShouldRight() {
        KdbRowValue kdbRowValue1 = new KdbRowValue(ColumnType.INTEGER, 1);
        assertEquals("1", kdbRowValue1.getStringValue());

        KdbRowValue kdbRowValue2 = new KdbRowValue(ColumnType.VARCHAR, "hello");
        assertEquals("hello", kdbRowValue2.getStringValue());
    }
}
