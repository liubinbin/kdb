package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.grpc.Header;
import cn.liubinbin.kdb.grpc.KdbSqlResponse;
import cn.liubinbin.kdb.grpc.Row;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.planer.*;
import cn.liubinbin.kdb.server.table.AbstTable;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.server.table.TableManage;
import cn.liubinbin.kdb.utils.Contants;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 16/10/31.
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
                List<Row> rows = new ArrayList<>();
                for (String tableName : tableNames){
                    rows.add(cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(tableName).build());
                }
                reply = KdbSqlResponse.newBuilder().setHeader(header).addAllRow(rows).build();
                break;
            case DESCRIBE_TABLE:
                header = Header.newBuilder().addHeader("column").addHeader("type").addHeader("parameter").build();
                DescribeTablePlan describeTablePlan = (DescribeTablePlan) plan;
                List<Column> columns = tableManage.describeTable(describeTablePlan.getTableName());
                List<Row> tableRows = new ArrayList<>();
                for (Column curColumn : columns) {
                    if (curColumn.getColumnType() == ColumnType.INTEGER) {
                        tableRows.add(cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(curColumn.getColumnName()).
                                addValue(curColumn.getColumnType().name()).addValue("0").build());
                    } else {
                        tableRows.add(cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(curColumn.getColumnName()).
                                addValue(curColumn.getColumnType().name()).addValue(curColumn.getColumnParameter().toString()).build());
                    }
                }
                reply = KdbSqlResponse.newBuilder().setHeader(header).addAllRow(tableRows).build();
                break;
            case CREATE_TABLE:
                CreateTablePlan createTablePlan = (CreateTablePlan) plan;
                System.out.println(createTablePlan);
                tableManage.createTable(createTablePlan.getTableName(), createTablePlan.getColumns());
                header = Header.newBuilder().addHeader(Contants.STATUS).build();
                cn.liubinbin.kdb.grpc.Row createTableRow = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(Contants.SUCCESS).build();
                reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(createTableRow).build();
                break;
            case INSERT_TABLE:
                System.out.println("this is insert table sql ");
                InsertTablePlan insertTablePlan = (InsertTablePlan) plan;
                KdbRow kdbRow = new KdbRow(insertTablePlan.getRowValueList());
                System.out.println("insertTablePlan  "  +  insertTablePlan);
                header = Header.newBuilder().addHeader("status").build();
                if(!tableManage.existTable(insertTablePlan.getTableName())) {
                    cn.liubinbin.kdb.grpc.Row tableNotExistRow = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(Contants.TABLE_NOT_EXIST).build();
                    reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(tableNotExistRow).build();
                } else {
                    tableManage.getTable(insertTablePlan.getTableName()).insert(kdbRow);
                    cn.liubinbin.kdb.grpc.Row insertSuccess = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(Contants.SUCCESS).build();
                    reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(insertSuccess).build();
                }
                break;
            case DELETE_TABLE:
                System.out.println("this is delete table sql ");
                DeleteTablePlan deleteTablePlan = (DeleteTablePlan) plan;
                header = Header.newBuilder().addHeader("status").build();
                System.out.println(deleteTablePlan);
                if(!tableManage.existTable(deleteTablePlan.getTableName())) {
                    cn.liubinbin.kdb.grpc.Row tableNotExistRow = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(Contants.TABLE_NOT_EXIST).build();
                    reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(tableNotExistRow).build();
                } else {
                    tableManage.getTable(deleteTablePlan.getTableName()).delete(deleteTablePlan.getWhereBoolExpreList());
                    cn.liubinbin.kdb.grpc.Row insertSuccess = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(Contants.SUCCESS).build();
                    reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(insertSuccess).build();
                }
                break;
            case SELECT_TABLE:
                SelectTablePlan selectTablePlan = (SelectTablePlan) plan;

                AbstTable table = tableManage.getTable(selectTablePlan.getTableName());
                AbstrExePlan selectPhysicalPlan = Engine.getInstance().generatePhysicalPlan(selectTablePlan, table);

                // 先生成物理计划
                List<KdbRow> kdbRows = new ArrayList<>();
                while (selectPhysicalPlan.hasMore()) {
                    kdbRows.add(selectPhysicalPlan.onNext());
                }



                System.out.println();
                header = Header.newBuilder().addHeader("bin header").build();
                cn.liubinbin.kdb.grpc.Row dataRow = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(kdbRows.get(0).getValues().get(0).getStringValue()).build();
                reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(dataRow).build();
                break;
        }
        return reply;
    }
}
