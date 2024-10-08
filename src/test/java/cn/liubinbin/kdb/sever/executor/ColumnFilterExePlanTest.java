package cn.liubinbin.kdb.sever.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.ColumnFilterExePlan;
import cn.liubinbin.kdb.server.executor.FakeScanExePlan;
import cn.liubinbin.kdb.server.table.BtreeTable;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


/**
 * @author liubinbin
 * @date 2024/09/01
 */
public class ColumnFilterExePlanTest {

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

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);
        assertFalse(columnFilterExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);

        assertTrue(columnFilterExePlan.hasNextPlan());
    }

    @Test
    public void onNextShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);

        KdbRow row = columnFilterExePlan.onNext();
        assertEquals(0, row.getRowKey().longValue());
    }

    @Test
    public void hasMoreShouldRight2() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);
        assertTrue(columnFilterExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight2() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);
        assertNotNull(columnFilterExePlan.onNext());
    }

    @Test
    public void filterShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);
        KdbRow kdbRow = columnFilterExePlan.onNext();
        assertEquals(1, kdbRow.getValues().size());
        assertEquals(0, kdbRow.getValues().get(0).getIntValue().longValue());
    }

    @Test
    public void countStartShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);

        BtreeTable btreeTable = getFakeTable();

        List<String> columnNameList = new ArrayList<>();
        columnNameList.add(Contants.COUNT_START);
        ColumnFilterExePlan columnFilterExePlan = new ColumnFilterExePlan(fakeScanExePlan, btreeTable, columnNameList);
        KdbRow kdbRow = columnFilterExePlan.onNext();
        assertEquals(1, kdbRow.getValues().size());
        assertEquals(3, kdbRow.getValues().get(0).getIntValue().longValue());
    }
}
