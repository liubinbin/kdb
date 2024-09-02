package cn.liubinbin.kdb.sever.executor;


import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.FakeScanExePlan;
import cn.liubinbin.kdb.server.executor.LimitExePlan;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class LimitExePlanTest {

    private List<KdbRow> getIdKdbRowList(int count){
        List<KdbRow> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            KdbRow row = new KdbRow();
            KdbRowValue id = new KdbRowValue(ColumnType.INTEGER, i);
            row.appendRowValue(id);
            data.add(row);
        }
        return data;
    }

    private List<KdbRow> getIdNameKdbRowList(int count){
        List<KdbRow> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            KdbRow row = new KdbRow();
            KdbRowValue id = new KdbRowValue(ColumnType.INTEGER, i);
            KdbRowValue name = new KdbRowValue(ColumnType.INTEGER, "bin" + i);
            row.appendRowValue(id);
            row.appendRowValue(name);
            data.add(row);
        }
        return data;
    }

    @Test
    public void hasMoreShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(0);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        LimitExePlan limitExePlan = new LimitExePlan(fakeScanExePlan, 1);
        assertFalse(limitExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(0);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        LimitExePlan limitExePlan = new LimitExePlan(fakeScanExePlan, 1);

        assertTrue(limitExePlan.hasNextPlan());
    }

    @Test
    public void onNextShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        LimitExePlan limitExePlan = new LimitExePlan(fakeScanExePlan, 1);

        KdbRow row = limitExePlan.onNext();
        assertEquals(0, row.getRowKey().longValue());
    }

    @Test
    public void hasMoreShouldRight2() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        LimitExePlan limitExePlan = new LimitExePlan(fakeScanExePlan, 1);

        assertTrue(limitExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight2() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        LimitExePlan limitExePlan = new LimitExePlan(fakeScanExePlan, 1);

        assertNotNull(limitExePlan.onNext());
    }

}
