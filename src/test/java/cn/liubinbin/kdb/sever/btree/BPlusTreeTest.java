package cn.liubinbin.kdb.sever.btree;

import cn.liubinbin.kdb.server.btree.BPlusTree;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BPlusTreeTest {

    @Test
    public void insertShouldRight() {
        BPlusTree bPlusTree = new BPlusTree(3, null);
        KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        bPlusTree.insert(curRow);
        List<KdbRow> rows = bPlusTree.rangeScan();
        assertEquals(1, rows.size());
    }

    @Test
    public void scanShouldRight() {
        BPlusTree bPlusTree = new BPlusTree(3, null);
        KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        bPlusTree.insert(curRow);
        List<KdbRow> rows = bPlusTree.rangeScan();
        assertEquals(1, rows.get(0).getRowKey().intValue());
    }

    @Test
    public void deleteShouldRight() {
        BPlusTree bPlusTree = new BPlusTree(3, null);
        KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        bPlusTree.insert(curRow);
        bPlusTree.delete(curRow);
        List<KdbRow> rows = bPlusTree.rangeScan();
        assertEquals(0, rows.size());
    }

    @Test
    public void splitShouldRight() {
        BPlusTree bPlusTree = new BPlusTree(3, null);
        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            bPlusTree.insert(curRow);
        }
        List<KdbRow> rows = bPlusTree.rangeScan();
        assertEquals(6, rows.size());
    }

    @Test
    public void mergeShouldRight() {
        BPlusTree bPlusTree = new BPlusTree(3, null);
        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            bPlusTree.insert(curRow);
        }
        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            bPlusTree.delete(curRow);
        }
        List<KdbRow> rows = bPlusTree.rangeScan();
        assertEquals(0, rows.size());

        assertTrue(bPlusTree.getRoot().isLeaf());
    }

    @Test
    public void duplicateShouldNotInsertRight() {
        BPlusTree bPlusTree = new BPlusTree(3, null);
        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            bPlusTree.insert(curRow);
        }
        KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        try{
            bPlusTree.insert(curRow);
        } catch (Exception e) {
            System.out.println("duplicate insert should throw exception");
        }

        List<KdbRow> rows = bPlusTree.rangeScan();
        assertEquals(6, rows.size());
    }
}
