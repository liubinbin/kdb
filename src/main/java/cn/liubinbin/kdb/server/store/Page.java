package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.ByteArrayUtils;
import cn.liubinbin.kdb.utils.ByteUtils;
import cn.liubinbin.kdb.utils.Contants;

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
    private int curOffset;
    private String tableName;
    private List<Column> tableColumnList;

    public Page(Node node, String tableName, List<Column> tableColumnList) {
        this.node = node;
        this.tableName = tableName;
        this.tableColumnList = tableColumnList;
        this.curOffset = 0;
        this.data = new byte[PAGE_SIZE];
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

    public void writeTo(Node node) {
        int offset = 0;
        int overviewStatus = node.getStatus();
        // meta data
        // meta.nodeId
        ByteArrayUtils.putInt(data, Contants.NODE_ID_SHIFT, node.getNodeId());
        // meta.status
        ByteArrayUtils.putInt(data, Contants.STATUS_SHIFT, overviewStatus);
        // meta.rowCount
        ByteArrayUtils.putInt(data, Contants.ROW_COUNT, node.getCurrentRowCount());
        // meta.childrenCount
        ByteArrayUtils.putInt(data, Contants.CHILDREN_COUNT_SHIFT, node.getChildrenCount());
        offset = Contants.CHILDREN_COUNT_SHIFT;
        // meta.children.nodeId
        for (int i = 0; i < node.getChildrenCount(); i++) {
            ByteArrayUtils.putInt(data, offset, node.getChildren()[i].getNodeId());
            offset += Contants.INTEGER_SHIFT;
        }
        // meta.children.sep
        ByteArrayUtils.putInt(data, offset, node.getChildrenCount() - 1);
        offset += Contants.INTEGER_SHIFT;
        Integer[] childrenSep = node.getChildrenSep();
        for (int i = 0; i < node.getChildrenCount() - 1; i++) {
            ByteArrayUtils.putInt(data, offset, childrenSep[i]);
            offset += Contants.INTEGER_SHIFT;
        }
        // row data
        offset = Contants.META_SHIFT;
        KdbRow[] kdbRows = node.getData();
        for (int i = 0; i < node.getCurrentRowCount(); i++) {
            KdbRow kdbRow = kdbRows[i];
            // row.values
            List<KdbRowValue> values = kdbRow.getValues();
            for (int j = 0; j < values.size(); j++) {
                KdbRowValue value = values.get(j);
                if (value.getColumnType() == ColumnType.INTEGER) {
                    ByteArrayUtils.putInt(data, offset, value.getIntValue());
                    offset += Contants.INTEGER_SHIFT;
                } else if (value.getColumnType() == ColumnType.VARCHAR) {
                    ByteArrayUtils.putInt(data, offset, value.getStringValue().getBytes().length);
                    offset += Contants.INTEGER_SHIFT;
                    ByteArrayUtils.putBytes(data, offset, value.getStringValue().getBytes());
                    offset += tableColumnList.get(j).getColumnParameter();
                }
            }
        }
    }

    public Node readFrom() {
        int offset = 0;
        // meta data
        // meta.nodeId
        Integer nodeId = ByteArrayUtils.toInt(data, Contants.NODE_ID_SHIFT);
        Integer overviewStatus = ByteArrayUtils.toInt(data, Contants.STATUS_SHIFT);
        // meta.status
        boolean isRoot = ByteUtils.getBitAsBool(overviewStatus, Contants.ROOT_BIT_SHIFT);
        boolean isLeaf = ByteUtils.getBitAsBool(overviewStatus, Contants.LEAF_BIT_SHIFT);
        Node node = new Node(isRoot, isLeaf, nodeId);
        // meta.rowCount
        Integer rowCount = ByteArrayUtils.toInt(data, Contants.ROW_COUNT);
        // meta.children
        Integer childrenCount = ByteArrayUtils.toInt(data, Contants.CHILDREN_COUNT_SHIFT);
        offset = Contants.CHILDREN_COUNT_SHIFT;
        for (int i = 0; i < childrenCount; i++){
            System.out.println("NodeId " + ByteArrayUtils.toInt(data, offset));
            offset += Contants.INTEGER_SHIFT;
        }
        // meta.children.sep
        Integer childrenSepCount = ByteArrayUtils.toInt(data, offset);
        for (int i = 0; i < childrenSepCount; i++){
            System.out.println("childrenSep " + ByteArrayUtils.toInt(data, offset));
            offset += Contants.INTEGER_SHIFT;
        }
        // row data
        offset = Contants.META_SHIFT;
        KdbRow[] kdbRows = new KdbRow[];
        for (int i = 0; i < rowCount; i++) {
            KdbRow temp = new KdbRow();
            for (Column column: tableColumnList) {
                if (column.getColumnType() == ColumnType.INTEGER) {
                    temp.appendRowValue(new KdbRowValue(column.getColumnType(), ByteArrayUtils.toInt(data, offset)));
                    offset += Contants.INTEGER_SHIFT;
                } else if (column.getColumnType() == ColumnType.VARCHAR) {
                    Integer length = ByteArrayUtils.toInt(data, offset);
                    offset += Contants.INTEGER_SHIFT;
                    byte[] bytes = ByteArrayUtils.getBytes(data, offset, length);
                    temp.appendRowValue(new KdbRowValue(column.getColumnType(), new String(bytes)));
                    offset += column.getColumnParameter();
                }
            }
        }
        node.setData(kdbRows);
        return node;
    }

    public static void main(String[] args) {
        Node node = new Node(true, true, 1);
        List<Column> tableColumn = new ArrayList<>();
        tableColumn.add(new Column(0, "id", ColumnType.INTEGER, 0));
        tableColumn.add(new Column(1, "name", ColumnType.VARCHAR, 128));

        System.out.println(node);
        Page page = new Page(node, "test", tableColumn);
        // TODO
    }
}
