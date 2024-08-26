package cn.liubinbin.kdb.server.planer;

import cn.liubinbin.kdb.server.entity.KdbRowValue;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/23.
 */
public class BoolExpression {

    private String columnName;
    private OperatorKind operator;
    private KdbRowValue value;

    public BoolExpression(String columnName, OperatorKind operator, KdbRowValue value) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public String toString() {
        return "BoolExpression{" +
                "columnName='" + columnName + '\'' +
                ", operator=" + operator +
                ", groupBy=" + value +
                '}';
    }
}
