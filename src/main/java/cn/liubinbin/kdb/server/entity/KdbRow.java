package cn.liubinbin.kdb.server.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 */
public class KdbRow {

    List<KdbRowValue> values;

    public KdbRow() {
        this.values = new ArrayList<>();
    }

    public KdbRow(List<KdbRowValue> values) {
        this.values = values;
    }

    public void appendRowValue(KdbRowValue temp) {
        values.add(temp);
    }

    public List<KdbRowValue> getValues() {
        return values;
    }

    public int compareTo(KdbRow o) {
        return this.values.get(0).compareTo(o.values.get(0));
    }
}
