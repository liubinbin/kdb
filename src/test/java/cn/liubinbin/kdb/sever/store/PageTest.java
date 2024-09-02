package cn.liubinbin.kdb.sever.store;


import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.store.Page;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class PageTest {

    private KdbRow newMockRow(int intData, String strData) {
        List<KdbRowValue> values = new ArrayList<>();
        values.add(new KdbRowValue(ColumnType.INTEGER, intData));
        values.add(new KdbRowValue(ColumnType.VARCHAR, strData));
        return new KdbRow(values);
    }

    @Test
    public void testPageDataCompressAndDecompress() {
        Integer order = 4;
        Node node = new Node(true, true, 0, order);

        KdbRow rowOne = newMockRow(1, "Helloworld");
        KdbRow rowTwo = newMockRow(2, "wallstreet");
        KdbRow rowThree = newMockRow(3, "stockmarket");
        KdbRow rowFour = newMockRow(4, "kdb");
        KdbRow rowFive = newMockRow(5, "memory");
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowThree);
        node.add(rowFour);
        node.add(rowFive);
        node.treePrint();

        List<Column> tableColumn = new ArrayList<>();
        tableColumn.add(new Column(0, "id", ColumnType.INTEGER, 0));
        tableColumn.add(new Column(1, "name", ColumnType.VARCHAR, 128));
        Page page = new Page(node, tableColumn);
        page.compressNodeToBytes(node);

        Node newNode = page.exactFromBytes(order);

        assertEquals(newNode.getCurrentRowCount().longValue(), 3);
        assertTrue(newNode.isRoot());
        assertTrue(newNode.isLeaf());
        assertEquals(newNode.getData()[0].getRowKey().longValue(), 1);
    }

    @Test
    public void testPageFileReadAndWrite() throws Exception {
        String tempPath = "/tmp/test01";
        Integer order = 4;
        Node node = new Node(true, true, 0, order);

        KdbRow rowOne = newMockRow(1, "Helloworld");
        KdbRow rowTwo = newMockRow(2, "wallstreet");
        KdbRow rowThree = newMockRow(3, "stockmarket");
        KdbRow rowFour = newMockRow(4, "kdb");
        KdbRow rowFive = newMockRow(5, "memory");
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowThree);
        node.add(rowFour);
        node.add(rowFive);
        node.treePrint();

        List<Column> tableColumn = new ArrayList<>();
        tableColumn.add(new Column(0, "id", ColumnType.INTEGER, 0));
        tableColumn.add(new Column(1, "name", ColumnType.VARCHAR, 128));
        Page page = new Page(node, tableColumn);
        page.compressNodeToBytes(node);

        try (RandomAccessFile raf = new RandomAccessFile(new File(tempPath), "rw")) {
            page.writeTo(raf, 0);
        }

        Page newPage = new Page(new Node(true, true, 0, order), tableColumn);
        try (RandomAccessFile raf = new RandomAccessFile(new File(tempPath), "rw")) {
            newPage.readFrom(raf, 0);
        }
        Node newNode = newPage.exactFromBytes(order);
        assertEquals(newNode.getCurrentRowCount().longValue(), 3);
        assertTrue(newNode.isRoot());
        assertTrue(newNode.isLeaf());
        assertEquals(newNode.getData()[0].getRowKey().longValue(), 1);

        new File(tempPath).delete();
    }

}
