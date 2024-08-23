package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.btree.BPlusTree;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.planer.BoolExpression;
import cn.liubinbin.kdb.utils.Contants;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/16
 */
public class BtreeTable extends AbstTable {

    private final BPlusTree bPlusTree;

    public BtreeTable(String tableName, List<Column> columns, Integer order) {
        this(tableName, columns, TableType.Btree, order);
    }

    public BtreeTable(String tableName, List<Column> columns) {
        this(tableName, columns, TableType.Btree, Contants.DEFAULT_KDB_SERVER_TABLE_ENGINE_BTREE_ORDER);
    }

    public BtreeTable(String tableName, List<Column> columns, TableType tableType, Integer order) {
        super(tableName, columns, tableType);
        this.bPlusTree = new BPlusTree(order);
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
    public void print() {
        bPlusTree.print();
    }

    @Override
    public void insert(KdbRow rowToInsert) {
        System.out.println("start to insert row");
        bPlusTree.insert(rowToInsert);
        bPlusTree.print();
    }

    @Override
    public void delete(List<BoolExpression> expressions) {
        System.out.println("start to delete row");
//        bPlusTree.delete(null);
        // TODO
        bPlusTree.print();
    }

    @Override
    public void select() {

    }
}
