package cn.liubinbin.kdb.server.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 */
public class KdbRow {

    private Integer rowKey;
    private List<KdbRowValue> values;

    public KdbRow() {
        this.values = new ArrayList<>();
        this.rowKey = Integer.MIN_VALUE;
    }

    public KdbRow(List<KdbRowValue> values) {
        this.values = values;
        this.rowKey = values.get(0).getIntValue();
    }

    public void appendRowValue(KdbRowValue temp) {
        values.add(temp);
    }

    public List<KdbRowValue> getValues() {
        return values;
    }

    public Integer getRowKey() {
        return rowKey;
    }

    public int compareTo(KdbRow o) {
        return this.rowKey.compareTo(o.getRowKey());
    }
}
