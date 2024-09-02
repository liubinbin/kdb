package cn.liubinbin.kdb.sever.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.FakeScanExePlan;
import cn.liubinbin.kdb.server.executor.WhereExePlan;
import cn.liubinbin.kdb.server.planer.BoolExpression;
import cn.liubinbin.kdb.server.planer.OperatorKind;
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
public class WhereExePlanTest {

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
            KdbRowValue name = new KdbRowValue(ColumnType.VARCHAR, "bin" + i);
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

    private List<BoolExpression> getIdBoolExpreList(){
        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("id", OperatorKind.EQUAL, new KdbRowValue(ColumnType.INTEGER, 1));
        boolExpressions.add(boolExpression);
        return boolExpressions;
    }

    @Test
    public void hasMoreShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(0);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = getIdBoolExpreList();

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        assertFalse(whereExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(0);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = getIdBoolExpreList();

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        assertTrue(whereExePlan.hasNextPlan());
    }

    @Test
    public void onNextShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("id", OperatorKind.EQUAL, new KdbRowValue(ColumnType.INTEGER, 1));
        boolExpressions.add(boolExpression);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        KdbRow row = whereExePlan.onNext();
        assertEquals(1, row.getRowKey().longValue());
    }

    @Test
    public void hasMoreShouldRight2() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("id", OperatorKind.EQUAL, new KdbRowValue(ColumnType.INTEGER, 1));
        boolExpressions.add(boolExpression);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        assertTrue(whereExePlan.hasMore());
    }

    @Test
    public void hasNextShouldRight2() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("id", OperatorKind.EQUAL, new KdbRowValue(ColumnType.INTEGER, 1));
        boolExpressions.add(boolExpression);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        assertNotNull(whereExePlan.onNext());
    }

    @Test
    public void greaterThanShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("id", OperatorKind.GREATER_THAN, new KdbRowValue(ColumnType.INTEGER, 1));
        boolExpressions.add(boolExpression);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        KdbRow row = whereExePlan.onNext();
        assertEquals(2, row.getRowKey().longValue());
    }

    @Test
    public void lessThanShouldRight() {
        List<KdbRow> kdbRows = getIdKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("id", OperatorKind.LESS_THAN, new KdbRowValue(ColumnType.INTEGER, 1));
        boolExpressions.add(boolExpression);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        KdbRow row = whereExePlan.onNext();
        assertEquals(0, row.getRowKey().longValue());
    }

    @Test
    public void stringCompareShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression = new BoolExpression("name", OperatorKind.EQUAL, new KdbRowValue(ColumnType.VARCHAR, "bin1"));
        boolExpressions.add(boolExpression);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, true);

        KdbRow row = whereExePlan.onNext();
        assertEquals(1, row.getRowKey().longValue());
    }

    @Test
    public void orShouldRight() {
        List<KdbRow> kdbRows = getIdNameKdbRowList(3);
        FakeScanExePlan fakeScanExePlan = new FakeScanExePlan(null, kdbRows);
        BtreeTable btreeTable = getFakeTable();

        List<BoolExpression> boolExpressions = new ArrayList<>();
        BoolExpression boolExpression1 = new BoolExpression("id", OperatorKind.EQUAL, new KdbRowValue(ColumnType.INTEGER, 0));
        BoolExpression boolExpression2 = new BoolExpression("name", OperatorKind.EQUAL, new KdbRowValue(ColumnType.VARCHAR, "bin1"));
        boolExpressions.add(boolExpression1);
        boolExpressions.add(boolExpression2);

        WhereExePlan whereExePlan = new WhereExePlan(fakeScanExePlan, btreeTable, boolExpressions, false);

        KdbRow row = whereExePlan.onNext();
        assertEquals(0, row.getRowKey().longValue());
        row = whereExePlan.onNext();
        assertEquals(1, row.getRowKey().longValue());
    }
}
