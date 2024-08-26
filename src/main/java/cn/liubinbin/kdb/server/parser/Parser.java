package cn.liubinbin.kdb.server.parser;

import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.planer.BoolExpression;
import cn.liubinbin.kdb.server.planer.OperatorKind;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/14.
 */
public class Parser {

    public static SqlNode parse(String sql) {
        SqlParser.ConfigBuilder configBuilder = SqlParser.create(sql).configBuilder()
                .setCaseSensitive(false)
                .setLex(Lex.MYSQL)
                .setParserFactory(SqlDdlParserImpl.FACTORY);
        SqlParser parser = SqlParser.create(sql, configBuilder.build());
        try {
            return parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("Error parsing SQL: " + sql, e);
        }
    }

    public static void main(String[] args) {
        List<String> sqls = new ArrayList<>();
//        sqls.add("describe database kdb");
//        sqls.add("describe table a");
//        sqls.add("create table a(id int, b int, c varchar(253))"); // id为主键
//        sqls.add("insert into a (id, name) VALUES (1, 'Alice')");

//        sqls.add("select * from a");
//        sqls.add("select * from a where b = 1");
//        sqls.add("select * from a where b = 1 and c = 'haha'");
//        sqls.add("select id,name from stu where id = 1 and name = 'haha'");

//        sqls.add("select age, count(*) from stu group by age"); // 不支持

        sqls.add("select * from stu where id = 1 order by b limit 10");
        sqls.add("select id,name from stu where id = 1 order by b limit 10");

//        sqls.add("delete from a where id = 3 and name = 'haha'");


        for (String sql : sqls) {
            System.out.println("--- parse one sql ---");
            SqlNode sqlNode = parse(sql);
            System.out.println(sqlNode.getKind());

            SqlOrderBy orderby = (SqlOrderBy) sqlNode;
            System.out.println(orderby.orderList);
            System.out.println(orderby.fetch);
            System.out.println(orderby.query.getClass());


//            SqlSelect select = (SqlSelect) sqlNode;
//            System.out.println("sqlNode select " + select);
//            System.out.println("1: " + select.getSelectList());
//            List<String> columnList = ParserUtils.getColumnList(select.getSelectList());
//            System.out.println(columnList);
//            System.out.println("2: " + select.getFrom().toString());
//            System.out.println("3: " + select.getWhere().getClass());
//            SqlBasicCall curCondition = (SqlBasicCall) select.getWhere();

//            SqlBasicCall curCondition = (SqlBasicCall) sqlBasicCall;
//            if (curCondition.getOperator().kind == SqlKind.AND || curCondition.getOperator().kind == SqlKind.OR) {
//                List<BoolExpression> whereBoolExpreList = new ArrayList<>();
//                for (SqlNode curNode : curCondition.getOperandList()) {
//                    SqlBasicCall curCondition1 = (SqlBasicCall) curNode;
//                    String columnName = curCondition1.getOperandList().get(0).toString();
//                    KdbRowValue curKdbRowValue = ParserUtils.getRowValue(curCondition1.getOperandList().get(1));
//                    switch (curCondition1.getOperator().kind) {
//                        case EQUALS:
//                            whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.EQUAL, curKdbRowValue));
//                            break;
//                        case GREATER_THAN:
//                            whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.GREATER_THAN, curKdbRowValue));
//                            break;
//                        case LESS_THAN:
//                            whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.LESS_THAN, curKdbRowValue));
//                            break;
//                    }
//                }
//                System.out.println("lbb whereBoolExpreList " + whereBoolExpreList);
//            } else {
//                String columnName = curCondition.getOperandList().get(0).toString();
//                List<BoolExpression> whereBoolExpreList = new ArrayList<>();
//                KdbRowValue curKdbRowValue = ParserUtils.getRowValue(curCondition.getOperandList().get(1));
//                switch (curCondition.getOperator().kind) {
//                    case EQUALS:
//                        whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.EQUAL, curKdbRowValue));
//                        break;
//                    case GREATER_THAN:
//                        whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.GREATER_THAN, curKdbRowValue));
//                        break;
//                    case LESS_THAN:
//                        whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.LESS_THAN, curKdbRowValue));
//                        break;
//                }
//                System.out.println("lbb whereBoolExpreList " + whereBoolExpreList);
//            }
//            System.out.println("4: " + select.getOrderList());
//            System.out.println("5: " + select.getGroup());
//            System.out.println("6: " + select.getOffset());

//            SqlOrderBy orderby = (SqlOrderBy) sqlNode;
//            System.out.println(orderby.fetch.getClass());

//            SqlDelete delete = (SqlDelete) sqlNode;
//            System.out.println(delete.getTargetTable().toString());
//
//            SqlBasicCall conditionList = (SqlBasicCall) delete.getCondition();
//            List<BoolExpression> whereBoolExpreList = new ArrayList<>();
//            if (conditionList != null) {
//                for (SqlNode node : conditionList.getOperandList()) {
//                    SqlBasicCall curCondition = (SqlBasicCall) node;
//                    String columnName = curCondition.getOperandList().get(0).toString();
//                    KdbRowValue curKdbRowValue = ParserUtils.getRowValue(curCondition.getOperandList().get(1));
//                    switch (curCondition.getOperator().kind) {
//                        case EQUALS:
//                            whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.EQUAL, curKdbRowValue));
//                            break;
//                        case GREATER_THAN:
//                            whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.GREATER_THAN, curKdbRowValue));
//                            break;
//                        case LESS_THAN:
//                            whereBoolExpreList.add(new BoolExpression(columnName, OperatorKind.LESS_THAN, curKdbRowValue));
//                            break;
//                    }
//                }
//            }


//            SqlNode sqlNode = parse(sql);
//            SqlInsert insert = (SqlInsert) sqlNode;
//            System.out.println(insert);
//
//            System.out.println(insert.getTargetTable().getClass());
//            String tableName = insert.getTargetTable().toString();
//            System.out.println("tableName " + tableName);
//            System.out.println(ParserUtils.getColumnList(insert.getTargetColumnList()));
//
//            SqlBasicCall c = (SqlBasicCall) insert.getSource();
//            SqlBasicCall d = (SqlBasicCall) c.getOperandList().get(0);
//            System.out.println(ParserUtils.getRowValueList(d));
//            System.out.println(ParserUtils.getColumnList(insert.getOperandList()));


//            SqlCreateTable create = (SqlCreateTable) sqlNode;
//            for (SqlNode node : create.columnList) {
//                System.out.println( " --- one colume ---");
//                SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) node;
//                System.out.println(columnDeclaration.dataType.getTypeNameSpec());
//                SqlDataTypeSpec sqlDataTypeSpec = columnDeclaration.dataType;
//                SqlBasicTypeNameSpec sqlBasicTypeNameSpec = (SqlBasicTypeNameSpec)sqlDataTypeSpec.getTypeNameSpec();
//                System.out.println(sqlBasicTypeNameSpec.getPrecision());
//                System.out.println(sqlBasicTypeNameSpec.getTypeName());
//            }
        }
    }
}
