package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.KdbRow;

import java.util.ArrayList;
import java.util.List;

public class FakeTable extends AbstTable {

    public FakeTable(String tableName, List<Column> columns) {
        super(tableName, columns);
    }

    @Override
    public List<KdbRow> limit(Integer limit) {
        List<KdbRow> list = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            KdbRow temp = new KdbRow();
            temp.appendRowValue(" " + i);
            list.add(temp);
        }
        return list;
    }
}
