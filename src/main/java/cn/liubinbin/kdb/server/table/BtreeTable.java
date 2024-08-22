package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.btree.BPlusTree;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/16
 */
public class BtreeTable extends AbstTable {

    private BPlusTree bPlusTree;

    public BtreeTable(String tableName, List<Column> columns) {
        super(tableName, columns, TableType.Btree);
    }

    public BtreeTable(String tableName, List<Column> columns, TableType tableType) {
        super(tableName, columns, tableType);
    }

    @Override
    public List<KdbRow> limit(Integer limit) {
        List<KdbRow> list = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            KdbRow temp = new KdbRow();
            temp.appendRowValue(new KdbRowValue(ColumnType.VARCHAR, "hello world " + i));
            list.add(temp);
        }
        return list;
    }

    @Override
    public void insert(KdbRow rowToInsert) {
        System.out.println("start to insert row");
    }
}
