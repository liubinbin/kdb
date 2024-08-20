package cn.liubinbin.kdb.server.entity;

import cn.liubinbin.kdb.server.table.ColumnType;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/8/15.
 */
public class KdbRowValue {

    private String StringValue;
    private Integer IntValue;
    private ColumnType columnType;

    public KdbRowValue(ColumnType columnType, String StringValue) {
        this.columnType = columnType;
        this.StringValue = StringValue;
    }

    public KdbRowValue(ColumnType columnType, Integer IntValue) {
        this.columnType = columnType;
        this.IntValue = IntValue;
    }

    public int compareTo(KdbRowValue o) {
        if (columnType == ColumnType.INTEGER) {
            return IntValue.compareTo(o.IntValue);
        } else {
            return StringValue.compareTo(o.StringValue);
        }
    }

    public Integer getIntValue() {
        return IntValue;
    }

    public String getStringValue() {
        if (columnType == ColumnType.INTEGER) {
            return Integer.toString(IntValue);
        }
        return StringValue;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    @Override
    public String toString() {
        return "KdbRowValue{" +
                "StringValue='" + StringValue + '\'' +
                ", IntValue=" + IntValue +
                ", type=" + columnType +
                '}';
    }

}
