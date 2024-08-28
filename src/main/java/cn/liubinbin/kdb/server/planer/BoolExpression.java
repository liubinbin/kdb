package cn.liubinbin.kdb.server.planer;

import cn.liubinbin.kdb.server.entity.KdbRowValue;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/23.
 */
public class BoolExpression {

    private String columnName;
    private Integer columnIdx;
    private OperatorKind operator;
    private KdbRowValue value;

    public BoolExpression(String columnName, OperatorKind operator, KdbRowValue value) {
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
        this.columnIdx = -1;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnIdx(Integer columnIdx) {
        this.columnIdx = columnIdx;
    }

    public Integer getColumnIdx() {
        return columnIdx;
    }

    public OperatorKind getOperator() {
        return operator;
    }

    public KdbRowValue getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "BoolExpression{" +
                "columnName='" + columnName + '\'' +
                ", operator=" + operator +
                ", groupBy=" + value +
                ", columnIdx=" + columnIdx +
                '}';
    }
}
