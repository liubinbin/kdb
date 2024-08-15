package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.grpc.Header;
import cn.liubinbin.kdb.grpc.KdbSqlResponse;
import cn.liubinbin.kdb.grpc.Row;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.planer.DescribeTablePlan;
import cn.liubinbin.kdb.server.planer.Plan;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.server.table.TableManage;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liubinbin on 16/10/31.
 */
public class Executor {

    private TableManage tableManage = null;

    public Executor(TableManage tableManage) {
        this.tableManage = tableManage;
    }

    public KdbSqlResponse execute(Plan plan) {
        Header header = Header.newBuilder().addHeader("null data").build();
        KdbSqlResponse reply = KdbSqlResponse.newBuilder().setHeader(header).build();
        switch (plan.getPlanKind()) {
            case DESCRIBE_DATABASE:
                header = Header.newBuilder().addHeader("tables").build();
                List<String> tableNames = tableManage.ListTableName();
                Row describeDatabaseRow = cn.liubinbin.kdb.grpc.Row.newBuilder().addAllValue(tableNames).build();
                reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(describeDatabaseRow).build();
                break;
            case DESCRIBE_TABLE:
                header = Header.newBuilder().addHeader("column").addHeader("type").addHeader("parameter").build();
                DescribeTablePlan describeTablePlan = (DescribeTablePlan) plan;
                List<Column> columns = tableManage.describeTable(describeTablePlan.getTableName());
                List<Row> rows = new ArrayList<>();
                for (Column curColumn : columns) {
                    if (curColumn.getColumnType() == ColumnType.INT) {
                        rows.add(cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(curColumn.getColumnName()).
                                addValue(curColumn.getColumnType().name()).addValue("0").build());
                    } else {
                        rows.add(cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(curColumn.getColumnName()).
                                addValue(curColumn.getColumnType().name()).addValue(curColumn.getColumnParameter().toString()).build());
                    }
                }
                reply = KdbSqlResponse.newBuilder().setHeader(header).addAllRow(rows).build();
                break;
            case SELECT_TABLE:
                List<KdbRow> kdbRows = tableManage.getTable("test").limit(1);
                header = Header.newBuilder().addHeader("bin header").build();
                cn.liubinbin.kdb.grpc.Row row = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(kdbRows.get(0).getValues().get(0)).build();
                reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(row).build();
                break;
        }
        return reply;
    }
}
