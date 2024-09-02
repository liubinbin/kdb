package cn.liubinbin.kdb.sever.executor;


import cn.liubinbin.kdb.server.executor.AbstrExePlan;
import cn.liubinbin.kdb.server.executor.Engine;
import cn.liubinbin.kdb.server.executor.ExePlanKind;
import cn.liubinbin.kdb.server.planer.SelectTablePlan;
import cn.liubinbin.kdb.server.table.BtreeTable;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertSame;


/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class EngineTest {

    private BtreeTable getFakeTable(){
        List<Column> tableColumn = new ArrayList<>();
        tableColumn.add(new Column(0, "id", ColumnType.INTEGER, 0));
        tableColumn.add(new Column(1, "name", ColumnType.VARCHAR, 128));

        return new BtreeTable("test", tableColumn);
    }

    @Test
    public void physicalPlanTypeShouldBeScan(){
        BtreeTable fakeTable = getFakeTable();
        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        SelectTablePlan plan = new SelectTablePlan("test", columnNameList, false, null);
        AbstrExePlan physicalPlan = Engine.getInstance().generatePhysicalPlan(plan, fakeTable);
        assertSame(physicalPlan.nextPlan.kind, ExePlanKind.ScanTable);
        assertSame(physicalPlan.kind, ExePlanKind.ColumnFilter);
    }

    @Test
    public void physicalPlanTypeShouldBeOrder(){
        BtreeTable fakeTable = getFakeTable();
        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        SelectTablePlan plan = new SelectTablePlan("test", columnNameList, false, null, "id", null);
        AbstrExePlan physicalPlan = Engine.getInstance().generatePhysicalPlan(plan, fakeTable);
        assertSame(physicalPlan.nextPlan.kind, ExePlanKind.OrderByTable);
    }

    @Test
    public void physicalPlanTypeShouldBeLimit(){
        BtreeTable fakeTable = getFakeTable();
        List<String> columnNameList = new ArrayList<>();
        columnNameList.add("id");
        columnNameList.add("name");
        SelectTablePlan plan = new SelectTablePlan("test", columnNameList, false, null, "id", 1);
        AbstrExePlan physicalPlan = Engine.getInstance().generatePhysicalPlan(plan, fakeTable);
        assertSame(physicalPlan.kind, ExePlanKind.Limit);
    }
}
