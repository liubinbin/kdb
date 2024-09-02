package cn.liubinbin.kdb.sever.executor;


import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.FakeScanExePlan;
import cn.liubinbin.kdb.server.executor.OrderByExePlan;
import cn.liubinbin.kdb.server.table.BtreeTable;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class OrderByExePlanTest {

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

    private List<KdbRow> getIdNameKdbRowListForOrderBy(int count){
        List<KdbRow> data = new ArrayList<>();
        for (int i = count; i > 0; i--) {
            KdbRow row = new KdbRow();
            KdbRowValue id = new KdbRowValue(ColumnType.INTEGER, i);
            KdbRowValue name = new KdbRowValue(ColumnType.INTEGER, "bin" + i);
            row.appendRowValue(id);
            row.appendRowValue(name);
            data.add(row);
        }
        return data;
    }


    private BtreeTable getFakeTable(){
        List<Column> tableColumn = new ArrayList<>();
        tableColumn.add(new Column(0, "id", ColumnType.INTEGER, 0));
        tableColumn.add(new Column(1, "name", ColumnType.VARCHAR, 128));

        return new BtreeTable("test", tableColumn);
    }

    @Test
    public void hasMoreShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(0);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        OrderByExePlan orderByExePlan = new OrderByExePlan(fakeScanExePlan, btreeTable, "id");
        assertTrue(orderByExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        OrderByExePlan orderByExePlan = new OrderByExePlan(fakeScanExePlan, btreeTable, "id");

        assertTrue(orderByExePlan.hasNextPlan());
    }

    @Test
    public void onNextShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowListForOrderBy(3);
        System.out.println(kdbRows);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        OrderByExePlan orderByExePlan = new OrderByExePlan(fakeScanExePlan, btreeTable, "id");

        KdbRow row = orderByExePlan.onNext();
        assertEquals(1, row.getRowKey().longValue());
    }

    @Test
    public void hasMoreShouldRight2() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        OrderByExePlan orderByExePlan = new OrderByExePlan(fakeScanExePlan, btreeTable, "id");
        assertTrue(orderByExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight2() {
        List<KdbRow> kdbRows = getIdNameKdbRowListForOrderBy(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        OrderByExePlan orderByExePlan = new OrderByExePlan(fakeScanExePlan, btreeTable, "id");
        assertNotNull(orderByExePlan.onNext());
    }

    @Test
    public void orderByShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowListForOrderBy(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        OrderByExePlan orderByExePlan = new OrderByExePlan(fakeScanExePlan, btreeTable, "id");

        KdbRow kdbRow = orderByExePlan.onNext();
        assertEquals(2, kdbRow.getValues().size());
        assertEquals(1, kdbRow.getValues().get(0).getIntValue().longValue());

        kdbRow = orderByExePlan.onNext();
        assertEquals(2, kdbRow.getValues().get(0).getIntValue().longValue());

        kdbRow = orderByExePlan.onNext();
        assertEquals(3, kdbRow.getValues().get(0).getIntValue().longValue());

        kdbRow = orderByExePlan.onNext();
        assertNull(kdbRow);

    }

}
