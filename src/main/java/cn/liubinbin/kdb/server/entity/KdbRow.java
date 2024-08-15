package cn.liubinbin.kdb.server.entity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liubinbin on 2020/1/16.
 */
public class KdbRow {

    List<String> values;

    public KdbRow() {
        this.values = new ArrayList<>();
    }

    public KdbRow(List<String> values) {
        this.values = values;
    }

    public void appendRowValue(String temp) {
        values.add(temp);
    }

    public List<String> getValues() {
        return values;
    }
}
