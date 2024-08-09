package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.Row;

import java.util.ArrayList;
import java.util.List;

public class FakeTable extends AbstTable implements Table {

    public FakeTable(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public List<Row> limit(Integer limit) {
        List<Row> list = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            Row temp = new Row();
            temp.appendRowValue(" " + i);
            list.add(temp);
        }
        return list;
    }
}
