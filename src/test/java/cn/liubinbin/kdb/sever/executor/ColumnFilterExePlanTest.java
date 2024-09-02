package cn.liubinbin.kdb.sever.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.FakeScanExePlan;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


/**
 * @author liubinbin
 * @date 2024/09/01
 */
public class ColumnFilterExePlanTest {

    private List<KdbRow> getKdbRowList(int count){
        List<KdbRow> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            KdbRow row = new KdbRow();
            KdbRowValue value = new KdbRowValue(ColumnType.INTEGER, i);
            row.appendRowValue(value);
            data.add(row);
        }
        return data;
    }

    @Test
    public void hasMoreShouldRight() {
        List<KdbRow> kdbRows = getKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
//        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, null, null);
//        assertFalse(columnFilterExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight() {
        List<KdbRow> kdbRows = new ArrayList<>();
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        assertFalse(fakeScanExePlan.hasNext());
    }

    @Test
    public void onNextShouldRight() {
        List<KdbRow> kdbRows = new ArrayList<>();
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        assertNull(fakeScanExePlan.onNext());
    }

    @Test
    public void hasMoreShouldRight2() {
        List<KdbRow> kdbRows = new ArrayList<>();
        kdbRows.add(new KdbRow());
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        assertTrue(fakeScanExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight2() {
        List<KdbRow> kdbRows = new ArrayList<>();
        kdbRows.add(new KdbRow());
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        assertNotNull(fakeScanExePlan.onNext());
    }
}
