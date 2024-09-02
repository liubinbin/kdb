package cn.liubinbin.kdb.server.entity;

import cn.liubinbin.kdb.server.table.ColumnType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 */
public class KdbRow implements Comparable<KdbRow>{

    private Integer rowKey;
    private List<KdbRowValue> values;

    public KdbRow() {
        this.values = new ArrayList<>();
        this.rowKey = Integer.MIN_VALUE;
    }

    public KdbRow(List<KdbRowValue> values) {
        this.values = values;
        if (values.get(0).getColumnType() == ColumnType.INTEGER) {
            this.rowKey = values.get(0).getIntValue();
        } else {
            this.rowKey = values.get(0).getStringValue().hashCode();
        }
    }

    public void appendRowValue(KdbRowValue temp) {
        values.add(temp);
        this.rowKey = values.get(0).getIntValue();
    }

    public List<KdbRowValue> getValues() {
        return values;
    }

    public Integer getRowKey() {
        return rowKey;
    }

    public void setValues(List<KdbRowValue> values) {
        this.values = values;
    }

    @Override
    public int compareTo(KdbRow o) {
        return this.rowKey.compareTo(o.getRowKey());
    }

    @Override
    public String toString() {
        return "KdbRow{" +
                "rowKey=" + rowKey +
                ", values=" + values +
                '}';
    }
}
