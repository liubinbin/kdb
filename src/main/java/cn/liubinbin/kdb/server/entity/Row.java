package cn.liubinbin.kdb.server.entity;

import java.util.ArrayList;
import java.util.List;

public class Row {

    List<String> values;

    public Row() {
        this.values = new ArrayList<>();
    }

    public void appendRowValue(String temp) {
        values.add(temp);
    }

    public List<String> getValues() {
        return values;
    }
}
