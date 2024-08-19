package cn.liubinbin.kdb.server.planer;

import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.parser.ParserUtils;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;

import java.util.ArrayList;
import java.util.List;

public class Planer {

    public static Plan plan(SqlNode sqlNode) {
        System.out.println("sqlNode kind : " + sqlNode.getKind());
        Plan plan = null;
        switch (sqlNode.getKind()) {
            case DESCRIBE_SCHEMA:
                if (sqlNode instanceof SqlDescribeSchema) {
                    SqlDescribeSchema describeSchema = (SqlDescribeSchema) sqlNode;
                    plan = new DescribeDatabasePlan(describeSchema.getSchema().getSimple());
                } else {
                    throw new RuntimeException("Expected an DESCRIBE_SCHEMA statement but got: " + sqlNode.getKind());
                }
                break;
            case DESCRIBE_TABLE:
                if (sqlNode instanceof SqlDescribeTable) {
                    SqlDescribeTable describeTable = (SqlDescribeTable) sqlNode;
                    plan = new DescribeTablePlan(describeTable.getTable().getSimple());
                } else {
                    throw new RuntimeException("Expected an DESCRIBE_TABLE statement but got: " + sqlNode.getKind());
                }
                break;
            case CREATE_TABLE:
                if (sqlNode instanceof SqlCreate) {
                    SqlCreateTable create = (SqlCreateTable) sqlNode;
                    List<Column> columns = new ArrayList<>();
                    for (int i = 0; i < create.columnList.size(); i++) {
                        SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) create.columnList.get(2);
                        SqlDataTypeSpec sqlDataTypeSpec = columnDeclaration.dataType;
                        SqlBasicTypeNameSpec sqlBasicTypeNameSpec = (SqlBasicTypeNameSpec)sqlDataTypeSpec.getTypeNameSpec();
                        columns.add(new Column(i, columnDeclaration.name.getSimple(),
                                ColumnType.getColumnType(sqlBasicTypeNameSpec.getTypeName().getSimple()),
                                sqlBasicTypeNameSpec.getPrecision()));
                    }
                    plan = new CreateTablePlan(create.name.getSimple(), columns);
                } else {
                    throw new RuntimeException("Expected an CREATE_TABLE statement but got: " + sqlNode.getKind());
                }
                break;
            case INSERT:
                if (sqlNode instanceof SqlInsert) {
                    SqlInsert insert = (SqlInsert) sqlNode;
                    String tableName = ParserUtils.getString(insert.getTargetTable());
                    List<String> columnList = ParserUtils.getColumnList(insert.getTargetColumnList());
                    SqlBasicCall c = (SqlBasicCall) insert.getSource();
                    SqlBasicCall d = (SqlBasicCall) c.getOperandList().get(0);
                    List<KdbRowValue> rowValueList = ParserUtils.getRowValueList(d);
                    plan = new InsertTablePlan(tableName, columnList, rowValueList);
                } else {
                    throw new RuntimeException("Expected an INSERT_TABLE statement but got: " + sqlNode.getKind());
                }
                break;
            case SELECT:
                System.out.println("this is table select");
                break;
            case OTHER:
                System.out.println("do not support");
                break;
        }

        return plan;
    }
}
