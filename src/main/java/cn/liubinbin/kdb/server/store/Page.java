package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.ByteArrayUtils;
import cn.liubinbin.kdb.utils.ByteUtils;
import cn.liubinbin.kdb.utils.Contants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 * @date 2024/08/16
 *
 * 一块连续内存，数据来自 Node，写入到文件中。
 * 关联一个 Node
 */
public class Page {

    public static final int PAGE_SIZE = 16 * 1024; // 16KB
    private byte[] data;
    private Node node;
    private int curPageOffset;
    private String tableName;
    private List<Column> tableColumnList;

    public Page(Node node, List<Column> tableColumnList) {
        this.node = node;
        this.tableColumnList = tableColumnList;
        this.curPageOffset = 0;
        this.data = new byte[PAGE_SIZE];
    }

    public Node getNode() {
        return node;
    }

    public Page(Node node, String tableName, List<Column> tableColumnList) {
       this(node, tableColumnList);
//        this.tableName = tableName;
    }

    public int getRowMaxSize() {
        // 根据 PAGE_SIZE 计算
        int rowMaxSize = PAGE_SIZE - Contants.META_SHIFT;
        int rowSize = 0;
        for (Column column : this.tableColumnList) {
            if (column.getColumnType() == ColumnType.INTEGER) {
                rowSize += Contants.INTEGER_SHIFT;
            } else if (column.getColumnType() == ColumnType.VARCHAR) {
                rowSize += Contants.INTEGER_SHIFT + column.getColumnParameter();
            }
        }
        return rowMaxSize / rowSize;
    }

    public int getPageSize() {
        return PAGE_SIZE;
    }

    public void compressNodeToBytes(Node node) {
        int curOffset = 0;
        int overviewStatus = node.getStatus();
        // meta data
        // meta.nodeId
        ByteArrayUtils.putInt(data, Contants.NODE_ID_SHIFT, node.getNodeId());
        // meta.status
        ByteArrayUtils.putInt(data, Contants.STATUS_SHIFT, overviewStatus);
        // meta.rowCount
        ByteArrayUtils.putInt(data, Contants.ROW_COUNT, node.getCurrentRowCount());
        // meta.next
        if (node.getNext() != null) {
            ByteArrayUtils.putInt(data, Contants.NEXT_NODE_ID_SHIFT, node.getNext().getNodeId());
        } else {
            ByteArrayUtils.putInt(data, Contants.NEXT_NODE_ID_SHIFT, Contants.NULL_NEXT_NODE_ID);
        }
        // meta.childrenCount
        ByteArrayUtils.putInt(data, Contants.CHILDREN_COUNT_SHIFT, node.getChildrenCount());
        curOffset = Contants.CHILDREN_COUNT_SHIFT + Contants.INTEGER_SHIFT;
        // meta.children.nodeId
        for (int i = 0; i < node.getChildrenCount(); i++) {
            ByteArrayUtils.putInt(data, curOffset, node.getChildren()[i].getNodeId());
            curOffset += Contants.INTEGER_SHIFT;
        }
        // meta.children.sep
        ByteArrayUtils.putInt(data, curOffset, node.getChildrenSepCount());
        curOffset += Contants.INTEGER_SHIFT;
        Integer[] childrenSep = node.getChildrenSep();
        for (int i = 0; i < node.getChildrenSepCount(); i++) {
            ByteArrayUtils.putInt(data, curOffset, childrenSep[i]);
            curOffset += Contants.INTEGER_SHIFT;
        }
        // row data
        curOffset = Contants.META_SHIFT;
        KdbRow[] kdbRows = node.getData();
        for (int i = 0; i < node.getCurrentRowCount(); i++) {
            KdbRow kdbRow = kdbRows[i];
            // row.values
            List<KdbRowValue> values = kdbRow.getValues();
            for (int j = 0; j < values.size(); j++) {
                KdbRowValue value = values.get(j);
                if (value.getColumnType() == ColumnType.INTEGER) {
                    ByteArrayUtils.putInt(data, curOffset, value.getIntValue());
                    curOffset += Contants.INTEGER_SHIFT;
                } else if (value.getColumnType() == ColumnType.VARCHAR) {
                    ByteArrayUtils.putInt(data, curOffset, value.getStringValue().getBytes().length);
                    curOffset += Contants.INTEGER_SHIFT;
                    ByteArrayUtils.putBytes(data, curOffset, value.getStringValue().getBytes());
                    curOffset += tableColumnList.get(j).getColumnParameter();
                }
            }
        }
    }

    public Node exactFromBytes(Integer order) {
        int curOffset = 0;
        // meta data
        // meta.nodeId
        Integer nodeId = ByteArrayUtils.toInt(data, Contants.NODE_ID_SHIFT);
        Integer overviewStatus = ByteArrayUtils.toInt(data, Contants.STATUS_SHIFT);
        // meta.status
        boolean isRoot = ByteUtils.getBitAsBool(overviewStatus, Contants.ROOT_BIT_SHIFT);
        boolean isLeaf = ByteUtils.getBitAsBool(overviewStatus, Contants.LEAF_BIT_SHIFT);
        Node node = new Node(isRoot, isLeaf, nodeId, order);
        // meta.rowCount
        Integer rowCount = ByteArrayUtils.toInt(data, Contants.ROW_COUNT);
        Integer nextNodeId = ByteArrayUtils.toInt(data, Contants.NEXT_NODE_ID_SHIFT);
        node.setNextNodeId(nextNodeId);
        // meta.children
        Integer[] childrenId = new Integer[order];

        Integer childrenCount = ByteArrayUtils.toInt(data, Contants.CHILDREN_COUNT_SHIFT);
        curOffset = Contants.CHILDREN_COUNT_SHIFT + Contants.INTEGER_SHIFT;
        // meta.child.nodeId
        for (int i = 0; i < childrenCount; i++){
            childrenId[i] = ByteArrayUtils.toInt(data, curOffset);
            curOffset += Contants.INTEGER_SHIFT;
        }
        // meta.children.sep
        Integer[] childrenSep = new Integer[order + 1];
        Integer childrenSepCount = ByteArrayUtils.toInt(data, curOffset);
        curOffset += Contants.INTEGER_SHIFT;
        for (int i = 0; i < childrenSepCount; i++){
            childrenSep[i] = ByteArrayUtils.toInt(data, curOffset);
            curOffset += Contants.INTEGER_SHIFT;
        }
        // row data
        curOffset = Contants.META_SHIFT;
        KdbRow[] kdbRows = new KdbRow[order - 1];
        for (int i = 0; i < rowCount; i++) {
            KdbRow temp = new KdbRow();
            for (Column column: tableColumnList) {
                if (column.getColumnType() == ColumnType.INTEGER) {
                    temp.appendRowValue(new KdbRowValue(column.getColumnType(), ByteArrayUtils.toInt(data, curOffset)));
                    curOffset += Contants.INTEGER_SHIFT;
                } else if (column.getColumnType() == ColumnType.VARCHAR) {
                    int length = ByteArrayUtils.toInt(data, curOffset);
                    curOffset += Contants.INTEGER_SHIFT;
                    byte[] bytes = ByteArrayUtils.getBytes(data, curOffset, length);
                    temp.appendRowValue(new KdbRowValue(column.getColumnType(), new String(bytes)));
                    curOffset += column.getColumnParameter();
                }
            }
            kdbRows[i] = temp;
        }
        if (node.isLeaf()) {
            node.updateData(kdbRows, rowCount);
        } else {
            node.setChildAndSepId(childrenId, childrenSep, childrenSepCount);
        }
        setNode(node);
        return node;
    }

    private void setNode(Node node) {
        this.node = node;
    }

    public void writeTo(RandomAccessFile file, int offset) throws IOException {
        compressNodeToBytes(node);
        file.seek(offset);
        file.write(this.data);
    }

    public void readFrom(RandomAccessFile file, int offset) throws IOException {
        file.seek(offset);
        file.read(this.data);
    }

    public static KdbRow newMockRow(int intData, String strData) {
        List<KdbRowValue> values = new ArrayList<>();
        values.add(new KdbRowValue(ColumnType.INTEGER, intData));
        values.add(new KdbRowValue(ColumnType.VARCHAR, strData));
        return new KdbRow(values);
    }

    public static void main(String[] args) throws IOException {
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

        try (RandomAccessFile raf = new RandomAccessFile(new File("/Users/liubinbin/Desktop/ok/test.file"), "rw")) {
            page.writeTo(raf, 0);
        }

        System.out.println("after exact");
        Page newPage = new Page(new Node(true, true, 0, order), tableColumn);
        try (RandomAccessFile raf = new RandomAccessFile(new File("/Users/liubinbin/Desktop/ok/test.file"), "rw")) {
            newPage.readFrom(raf, 0);
        }
        Node newNode = newPage.exactFromBytes(order);
        newNode.treePrint();
    }
}
