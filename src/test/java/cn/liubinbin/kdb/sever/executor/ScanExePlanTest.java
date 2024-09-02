package cn.liubinbin.kdb.sever.executor;


import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.ScanExePlan;
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
public class ScanExePlanTest {

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
        BtreeTable btreeTable = getFakeTable();
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        for (KdbRow kdbRow : kdbRows) {
            btreeTable.insert(kdbRow);
        }
        ScanExePlan scanExePlan = new ScanExePlan(null, btreeTable);

        assertTrue(scanExePlan.hasMore());
    }

    @Test
    public void hasNextPlanShouldRight() {
        BtreeTable btreeTable = getFakeTable();
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        for (KdbRow kdbRow : kdbRows) {
            btreeTable.insert(kdbRow);
        }
        ScanExePlan scanExePlan = new ScanExePlan(null, btreeTable);

        assertFalse(scanExePlan.hasNextPlan());
    }

    @Test
    public void onNextShouldRight() {
        BtreeTable btreeTable = getFakeTable();
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        for (KdbRow kdbRow : kdbRows) {
            btreeTable.insert(kdbRow);
        }
        ScanExePlan scanExePlan = new ScanExePlan(null, btreeTable);

        KdbRow row = scanExePlan.onNext();
        assertEquals(0, row.getRowKey().longValue());
    }

    @Test
    public void hasMoreShouldRight2() {
        BtreeTable btreeTable = getFakeTable();
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        for (KdbRow kdbRow : kdbRows) {
            btreeTable.insert(kdbRow);
        }
        ScanExePlan scanExePlan = new ScanExePlan(null, btreeTable);

        assertTrue(scanExePlan.hasMore());

        for (int i = 0; i < 3; i++){
            scanExePlan.onNext();
        }
        scanExePlan.onNext();
        assertFalse(scanExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight2() {
        BtreeTable btreeTable = getFakeTable();
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        for (KdbRow kdbRow : kdbRows) {
            btreeTable.insert(kdbRow);
        }
        ScanExePlan scanExePlan = new ScanExePlan(null, btreeTable);

        KdbRow kdbRow = scanExePlan.onNext();
        assertNotNull(scanExePlan.onNext());
        assertEquals(0, kdbRow.getRowKey().longValue());
    }

}
