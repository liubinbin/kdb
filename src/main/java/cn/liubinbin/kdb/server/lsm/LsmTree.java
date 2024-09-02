package cn.liubinbin.kdb.server.lsm;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.Engine;
import cn.liubinbin.kdb.server.table.ColumnType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class LsmTree extends Engine {

    ConcurrentSkipListMap<Integer, KdbRow> tree;

    public LsmTree() {
        super();
        this.tree = new ConcurrentSkipListMap<>();
    }

    public void insert(KdbRow rowToInsert) {
        tree.put(rowToInsert.getRowKey(), rowToInsert);
    }

    public void delete(KdbRow rowToInsert) {
        tree.remove(rowToInsert.getRowKey());
    }

    public List<KdbRow> rangeScan(Integer lowerBound, Integer upperBound) {
        return new ArrayList<>(tree.subMap(lowerBound, upperBound).values());
    }


    public static void main(String[] args) {
        LsmTree lsmTree = new LsmTree();

        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            lsmTree.insert(curRow);
        }

        List<KdbRow> rows = lsmTree.rangeScan(4, 6);
        System.out.println("rows size " + rows.size());
        for (KdbRow row : rows) {
            System.out.print(row.getRowKey() + ", ");
        }
    }

}
