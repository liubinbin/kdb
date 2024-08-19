package cn.liubinbin.kdb.server.entity;

import cn.liubinbin.kdb.server.table.ColumnType;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/8/15.
 */
public class KdbRowValue {

    private String StringValue;
    private Integer IntValue;
    private ColumnType type;

    public KdbRowValue(ColumnType type, String StringValue) {
        this.type = type;
        this.StringValue = StringValue;
    }

    public KdbRowValue(ColumnType type, Integer IntValue) {
        this.type = type;
        this.IntValue = IntValue;
    }

    public int compareTo(KdbRowValue o) {
        if (type == ColumnType.INTEGER) {
            return IntValue.compareTo(o.IntValue);
        } else {
            return StringValue.compareTo(o.StringValue);
        }
    }

    public Integer getIntValue() {
        return IntValue;
    }

    public String getStringValue() {
        if (type == ColumnType.INTEGER) {
            return Integer.toString(IntValue);
        }
        return StringValue;
    }

    @Override
    public String toString() {
        return "KdbRowValue{" +
                "StringValue='" + StringValue + '\'' +
                ", IntValue=" + IntValue +
                ", type=" + type +
                '}';
    }
}
