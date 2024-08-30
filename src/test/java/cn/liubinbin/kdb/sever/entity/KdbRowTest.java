package cn.liubinbin.kdb.sever.entity;


import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author liubinbin
 * @date 2024/08/30
 */
public class KdbRowTest {

    @Test
    public void appendShoulcdWork() {
        KdbRow row = new KdbRow();
        KdbRowValue value = new KdbRowValue(ColumnType.INTEGER, 1);
        row.appendRowValue(value);
        assertEquals(1, row.getValues().size());
    }

    @Test
    public void compareToShouldWork() {
        KdbRow row1 = new KdbRow();
        KdbRowValue value1 = new KdbRowValue(ColumnType.INTEGER, 1);
        row1.appendRowValue(value1);
        KdbRow row2 = new KdbRow();
        KdbRowValue value2 = new KdbRowValue(ColumnType.INTEGER, 2);
        row2.appendRowValue(value2);
        KdbRow row3 = new KdbRow();
        KdbRowValue value3 = new KdbRowValue(ColumnType.INTEGER, 2);
        row3.appendRowValue(value3);
        assertEquals(-1, row1.compareTo(row2));
        assertEquals(1, row2.compareTo(row1));
        assertEquals(0, row2.compareTo(row3));
    }
}
