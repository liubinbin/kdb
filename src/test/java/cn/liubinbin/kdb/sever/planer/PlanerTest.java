package cn.liubinbin.kdb.sever.planer;


import cn.liubinbin.kdb.server.parser.Parser;
import cn.liubinbin.kdb.server.planer.*;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class PlanerTest {

    private Plan parseAndGetPlan(String sql){
        SqlNode sqlNode = Parser.parse(sql);
        // planer
        Plan plan = Planer.plan(sqlNode);
        return plan;
    }

    @Test
    public void parseDescDbShouldRight() {
        String sql = "describe database kdb";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.DESCRIBE_DATABASE);

        DescribeDatabasePlan describeDatabasePlan = (DescribeDatabasePlan) plan;

        assertEquals(describeDatabasePlan.getDbName(), "kdb");
    }

    @Test
    public void parseDescTableShouldRight() {
        String sql = "describe table stu";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.DESCRIBE_TABLE);

        DescribeTablePlan describeTablePlan = (DescribeTablePlan) plan;
        assertEquals(describeTablePlan.getTableName(), "stu");
    }

    @Test
    public void parseInsertTableShouldRight() {
        String sql = "insert into stu(id, age, name) VALUES (1, 29, 'Alice')";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.INSERT_TABLE);

        InsertTablePlan insertTablePlan = (InsertTablePlan) plan;
        assertEquals(insertTablePlan.getTableName(), "stu");
        assertSame(insertTablePlan.getColumnList().size(), 3);
        assertSame(insertTablePlan.getRowList().size(), 1);
        assertSame(insertTablePlan.getRowList().get(0).getValues().size(), 3);
        assertSame(insertTablePlan.getRowList().get(0).getValues().get(0).getColumnType(), ColumnType.INTEGER);
        assertSame(insertTablePlan.getRowList().get(0).getValues().get(0).getIntValue(), 1);
    }

    @Test
    public void parseCreateTableShouldRight() {
        String sql = "create table stu(id int, age int, name varchar(256))";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.CREATE_TABLE);

        CreateTablePlan createTablePlan = (CreateTablePlan) plan;
        assertEquals(createTablePlan.getTableName(), "stu");
        assertSame(createTablePlan.getColumns().size(), 3);
        assertEquals(createTablePlan.getColumns().get(0).getColumnName(), "id");
        assertSame(createTablePlan.getColumns().get(0).getColumnType(), ColumnType.INTEGER);
        assertEquals(createTablePlan.getColumns().get(0).getColumnParameter().longValue(), -1);
    }

    @Test
    public void parseDeleteTableShouldRight() {
        String sql = "delete from stu where id = 1 and name = 'haha'";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.DELETE_TABLE);
        DeleteTablePlan deleteTablePlan = (DeleteTablePlan) plan;
        assertEquals(deleteTablePlan.getTableName(), "stu");
        assertSame(deleteTablePlan.getWhereBoolExpreList().size(), 2);
        assertEquals(deleteTablePlan.getWhereBoolExpreList().get(0).getColumnName(), "id");
        assertSame(deleteTablePlan.getWhereBoolExpreList().get(0).getOperator(), OperatorKind.EQUAL);
        assertEquals(deleteTablePlan.getWhereBoolExpreList().get(0).getValue().getIntValue().longValue(), 1);
    }

    @Test
    public void parseSelectCountStarShouldRight() {
        String sql = "select count(*) from stu";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertFalse(selectTablePlan.isStar());
    }

    @Test
    public void parseSelectStarShouldRight() {
        String sql = "select * from stu";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertTrue(selectTablePlan.isStar());
    }

    @Test
    public void parseSelectColumnShouldRight() {
        String sql = "select id,age from stu";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertEquals(selectTablePlan.getColumnList().size(), 2);
    }

    @Test
    public void parseSelectWhereShouldRight() {
        String sql = "select * from stu where id = 1 and name = 'haha'";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertEquals(selectTablePlan.getWhereBoolExpreList().size(), 2);
        assertEquals(selectTablePlan.getWhereBoolExpreList().get(0).getColumnName(), "id");
        assertSame(selectTablePlan.getWhereBoolExpreList().get(0).getOperator(), OperatorKind.EQUAL);
        assertEquals(selectTablePlan.getWhereBoolExpreList().get(0).getValue().getIntValue().longValue(), 1);
        assertTrue(selectTablePlan.isWhereAnd());
    }

    @Test
    public void parseSelectWhereOrShouldRight() {
        String sql = "select * from stu where id = 1 or name = 'haha'";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertEquals(selectTablePlan.getWhereBoolExpreList().size(), 2);
        assertEquals(selectTablePlan.getWhereBoolExpreList().get(0).getColumnName(), "id");
        assertSame(selectTablePlan.getWhereBoolExpreList().get(0).getOperator(), OperatorKind.EQUAL);
        assertEquals(selectTablePlan.getWhereBoolExpreList().get(0).getValue().getIntValue().longValue(), 1);
        assertFalse(selectTablePlan.isWhereAnd());
    }

    @Test
    public void parseSelectOrderByShouldRight() {
        String sql = "select * from stu order by age";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertEquals(selectTablePlan.getColumnOrderBy(), "age");
    }

    @Test
    public void parseSelectLimitShouldRight() {
        String sql = "select * from stu order by age limit 3";
        Plan plan = parseAndGetPlan(sql);

        assertSame(plan.getPlanKind(), PlanKind.SELECT_TABLE);
        SelectTablePlan selectTablePlan = (SelectTablePlan) plan;
        assertEquals(selectTablePlan.getLimit().longValue(), 3);
    }
}
