package cn.liubinbin.kdb.sever.btree;


import cn.liubinbin.kdb.server.btree.Cursor;
import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * @author liubinbin
 * @date 2024/08/30
 */
public class CursorTest {

    @Test
    public void should_return_true_when_hasMore() {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        Node node = new Node(true, true, Contants.ROOT_NODE_ID, 4);
        node.add(rowOne);
        node.add(rowTwo);
        Cursor cursor = new Cursor(node);
        assertTrue(cursor.hasMore());
    }

    @Test
    public void should_return_next_when_next() {
        KdbRow row1 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow row2 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        Node node1 = new Node(true, true, Contants.ROOT_NODE_ID, 4);
        node1.add(row1);
        node1.add(row2);

        KdbRow row3 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow row4 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        Node node2 = new Node(true, true, Contants.ROOT_NODE_ID + 1, 4);
        node2.add(row3);
        node2.add(row4);

        node1.setNext(node2);

        Cursor cursor = new Cursor(node1);
        assertEquals(1, cursor.next().getRowKey().longValue());
        assertEquals(2, cursor.next().getRowKey().longValue());
        assertTrue(cursor.hasMore());
    }

    @Test
    public void should_return_false_when_hasMore() {
        KdbRow row1 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow row2 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        Node node1 = new Node(true, true, Contants.ROOT_NODE_ID, 4);
        node1.add(row1);
        node1.add(row2);

        Cursor cursor = new Cursor(node1);
        cursor.next();
        cursor.next();

        assertNull(cursor.next());
        assertFalse(cursor.hasMore());
    }

    @Test
    public void should_return_null_when_next() {
        KdbRow row1 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow row2 = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        Node node1 = new Node(true, true, Contants.ROOT_NODE_ID, 4);
        node1.add(row1);
        node1.add(row2);

        Cursor cursor = new Cursor(node1);
        cursor.next();
        cursor.next();
        assertNull(cursor.next());
    }
}
