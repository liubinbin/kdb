package cn.liubinbin.kdb.server.planer;

import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.parser.ParserUtils;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import jdk.nashorn.internal.runtime.linker.LinkerCallSite;
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
                        SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) create.columnList.get(i);
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
            case DELETE:
                System.out.println("this is table delete");
                if (sqlNode instanceof SqlDelete) {
                    SqlDelete delete = (SqlDelete) sqlNode;
                    String tableName = ParserUtils.getString(delete.getTargetTable());
                    List<BoolExpression> whereBoolExpreList = new ArrayList<>();
                    SqlBasicCall conditionList = (SqlBasicCall) delete.getCondition();
                    if (conditionList != null) {
                        for (SqlNode node : conditionList.getOperandList()) {
                            SqlBasicCall curCondition = (SqlBasicCall) node;
                            String columnName = curCondition.getOperandList().get(0).toString();
                            KdbRowValue curKdbRowValue = ParserUtils.getRowValue(curCondition.getOperandList().get(1));
                            switch (curCondition.getOperator().kind) {
                                case EQUALS:
                                    whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.EQUAL, curKdbRowValue));
                                    break;
                                case GREATER_THAN:
                                    whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.GREATER_THAN, curKdbRowValue));
                                    break;
                                case LESS_THAN:
                                    whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.LESS_THAN, curKdbRowValue));
                                    break;
                            }
                        }
                    }
                    plan = new DeleteTablePlan(tableName, whereBoolExpreList);
                } else {
                    throw new RuntimeException("Expected an DELETE_TABLE statement but got: " + sqlNode.getKind());
                }
                break;
            case SELECT:
                System.out.println("this is table select");
                if (sqlNode instanceof SqlSelect) {
                    SqlSelect select = (SqlSelect) sqlNode;
                    String tableName = select.getFrom().toString();

                    List<String> columnList = ParserUtils.getColumnList(select.getSelectList());
                    if (columnList.size() == 1 && columnList.get(0).isEmpty()) {
                        columnList = new ArrayList<>();
                    }

                    SqlBasicCall curCondition = (SqlBasicCall) select.getWhere();

                    List<BoolExpression> whereBoolExpreList = new ArrayList<>();
                    boolean isWhereAnd = true;
                    if (curCondition != null) {
                        if (curCondition.getOperator().kind == SqlKind.AND || curCondition.getOperator().kind == SqlKind.OR) {
                            if (curCondition.getOperator().kind == SqlKind.OR) {
                                isWhereAnd = false;
                            }
                            for (SqlNode curNode : curCondition.getOperandList()) {
                                SqlBasicCall curCondition1 = (SqlBasicCall) curNode;
                                whereBoolExpreList.add(ParserUtils.getWhereBoolExpreList(curCondition1));
                            }
                        } else {
                            whereBoolExpreList.add(ParserUtils.getWhereBoolExpreList(curCondition));
                        }
                    }

                    plan = new SelectTablePlan(tableName, columnList, isWhereAnd, whereBoolExpreList);
                } else {
                    throw new RuntimeException("Expected an SELECT_TABLE statement but got: " + sqlNode.getKind());
                }
                break;
            case ORDER_BY:
                System.out.println("this is table order by");
                if (sqlNode instanceof SqlOrderBy) {
                    SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
                    SqlSelect select = (SqlSelect) orderBy.query;
                    String tableName = select.getFrom().toString();

                    List<String> columnList = ParserUtils.getColumnList(select.getSelectList());
                    if (columnList.size() == 1 && columnList.get(0).isEmpty()) {
                        columnList = new ArrayList<>();
                    }

                    SqlBasicCall curCondition = (SqlBasicCall) select.getWhere();

                    List<BoolExpression> whereBoolExpreList = new ArrayList<>();
                    boolean isWhereAnd = true;
                    if (curCondition != null) {
                        if (curCondition.getOperator().kind == SqlKind.AND || curCondition.getOperator().kind == SqlKind.OR) {
                            if (curCondition.getOperator().kind == SqlKind.OR) {
                                isWhereAnd = false;
                            }
                            for (SqlNode curNode : curCondition.getOperandList()) {
                                SqlBasicCall curCondition1 = (SqlBasicCall) curNode;
                                whereBoolExpreList.add(ParserUtils.getWhereBoolExpreList(curCondition1));
                            }
                        } else {
                            whereBoolExpreList.add(ParserUtils.getWhereBoolExpreList(curCondition));
                        }
                    }
                    String columnOrderBy = orderBy.orderList.get(0).toString();
                    Integer limit = orderBy.fetch.toString().isEmpty() ? null : Integer.parseInt(orderBy.fetch.toString());

                    plan = new SelectTablePlan(tableName, columnList, isWhereAnd, whereBoolExpreList, columnOrderBy, limit);
                } else {
                    throw new RuntimeException("Expected an ORDER_BY_TABLE statement but got: " + sqlNode.getKind());
                }
                break;
            case OTHER:
                System.out.println("do not support");
                break;
        }

        return plan;
    }
}
