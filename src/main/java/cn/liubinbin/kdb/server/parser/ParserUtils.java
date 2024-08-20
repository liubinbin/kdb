package cn.liubinbin.kdb.server.parser;


import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.apache.calcite.sql.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 * @date 2024/08/19
 */
public class ParserUtils {

    public static List<String> getColumnList(List<SqlNode> sqlNodeList) {
        List<String> columnNames = new ArrayList<>();
        if (sqlNodeList != null) {
            for (SqlNode sqlNode : sqlNodeList) {
                columnNames.add(getString(sqlNode));
            }
        }
        return columnNames;
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
