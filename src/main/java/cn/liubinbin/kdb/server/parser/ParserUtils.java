package cn.liubinbin.kdb.server.parser;


import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.planer.BoolExpression;
import cn.liubinbin.kdb.server.planer.OperatorKind;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;
import org.apache.calcite.sql.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 * @date 2024/08/19
 */
public class ParserUtils {

    public static BoolExpression getWhereBoolExpreList(SqlBasicCall curCondition) {
        BoolExpression boolExpression = null;
        String columnName = curCondition.getOperandList().get(0).toString();
        KdbRowValue curKdbRowValue = ParserUtils.getRowValue(curCondition.getOperandList().get(1));
        switch (curCondition.getOperator().kind) {
            case EQUALS:
                boolExpression = new BoolExpression(columnName, OperatorKind.EQUAL, curKdbRowValue);
                break;
            case GREATER_THAN:
                boolExpression =  new BoolExpression(columnName, OperatorKind.GREATER_THAN, curKdbRowValue);
                break;
            case LESS_THAN:
                boolExpression = new BoolExpression(columnName, OperatorKind.LESS_THAN, curKdbRowValue);
                break;
        }
        return boolExpression;
    }

    public static List<String> getColumnList(List<SqlNode> sqlNodeList) {
        List<String> columnNames = new ArrayList<>();
        if (sqlNodeList != null) {
            for (SqlNode sqlNode : sqlNodeList) {
                if (sqlNode instanceof SqlIdentifier) {
                    columnNames.add(getString(sqlNode));
                } else if (sqlNode instanceof SqlBasicCall){
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                    if (sqlBasicCall.toString().equals(Contants.COUNT_START)) {
                        columnNames.add(Contants.COUNT_START);
                    }
                }
            }
        }
        return columnNames;
    }

    public static KdbRowValue getRowValue(SqlNode node) {
        KdbRowValue kdbRowValue = null;
        if (node instanceof SqlNumericLiteral) {
            kdbRowValue = new KdbRowValue(ColumnType.INTEGER, ((SqlNumericLiteral) node).intValue(false));
        } else if (node instanceof SqlCharStringLiteral ) {
            kdbRowValue = new KdbRowValue(ColumnType.VARCHAR, ((SqlCharStringLiteral) node).getStringValue());
        }

        return kdbRowValue;
    }

    public static List<KdbRowValue> getRowValueList(SqlBasicCall sqlBasicCall) {
        List<KdbRowValue> list = new ArrayList<>();
        for (SqlNode node : sqlBasicCall.getOperandList()) {
            KdbRowValue kdbRowValue = null;
            if (node instanceof SqlNumericLiteral) {
                kdbRowValue = new KdbRowValue(ColumnType.INTEGER, ((SqlNumericLiteral) node).intValue(false));
            } else if (node instanceof SqlCharStringLiteral ) {
                kdbRowValue = new KdbRowValue(ColumnType.VARCHAR, ((SqlCharStringLiteral) node).getStringValue());
            }
            list.add(kdbRowValue);
        }
        return list;
    }

    public static String getString(SqlNode sqlNode) {
        SqlIdentifier sqlIde = (SqlIdentifier) sqlNode;
        return sqlIde.getSimple();
    }
}
