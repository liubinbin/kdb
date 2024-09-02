package cn.liubinbin.kdb.sever.lsm;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.lsm.LsmTree;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author liubinbin
 * @date 2024/08/30
 */
public class LsmTest {
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        LsmTree lsmTree = new LsmTree();

        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            lsmTree.insert(curRow);
        }

        List<KdbRow> rows = lsmTree.rangeScan(Integer.MIN_VALUE, Integer.MAX_VALUE);
        assertEquals(6, rows.size());

        rows = lsmTree.rangeScan(4, 6);

        assertEquals(4, rows.get(0).getRowKey().longValue());
    }
}
