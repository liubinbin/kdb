package cn.liubinbin.kdb.server.btree;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.ByteUtils;
import cn.liubinbin.kdb.utils.Contants;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author liubinbin
 * B树节点实现
 */
public class Node {

    private boolean isRoot;
    private boolean isLeaf;
    // nodeId 和 pageId 合并为一个
    private Integer nodeId;

    // 具体数据内容
    private KdbRow[] data;
    // 最多的记录树，需从 PageSize 计算
    private Integer maxCount;
    // 当前记录数
    private Integer curRowCount;
    // 子节点
    private Node[] children;
    // 字节点个数
    private int childrenSepCount;

    // 字节点分割
    private Integer[] childrenSep;
    // key 的最大值
    private Integer maxKey;
    // key 的最小值
    private Integer minKey;
    // 父节点
    private Node parent;
    // order 度
    private Integer order;
    // 下面一个
    private Node next;

    public Node(boolean isRoot, boolean isLeaf, Integer nodeId, Integer order) {
        this.order = order;
        this.isRoot = isRoot;
        if (this.isRoot) {
            this.nodeId = 0;
        } else {
            this.nodeId = nodeId;
        }
        this.isLeaf = isLeaf;
        this.nodeId = nodeId;
        this.data = new KdbRow[order - 1];
        this.curRowCount = 0;
        this.maxKey = Integer.MIN_VALUE;
        this.minKey = Integer.MAX_VALUE;
        this.maxCount = order - 1;
        // childrenSep 实际长度只能为的 order -1，children 实际长度只能为 order。
        this.childrenSep = new Integer[order];
        this.children = new Node[order + 1];
        this.childrenSepCount = 0;
        this.next = null;
    }

    public void removeBigThan(KdbRow row) {
        int i = 0;
        while (i < curRowCount && data[i].compareTo(row) < 0) {
            i++;
        }
        if (i < curRowCount) {
            for (int j = i; j < curRowCount - 1; j++) {
                data[j] = null;
            }
        }
        curRowCount = i;
        updateMinAndMax();
    }

    public void removeSmallThan(KdbRow row) {
        int i = 0;
        while (i < curRowCount && data[i].compareTo(row) < 0) {
            i++;
        }
        int newNodeIdx = 0;
        if (i < curRowCount) {
            for (int j = i; j < curRowCount; j++) {
                data[newNodeIdx++] = data[j];
            }
        }
        curRowCount = newNodeIdx;
        updateMinAndMax();
    }

    /**
     * 返回 0，则删除成功无需merge
     * 返回 -1，则需 merge
     * @param row
     * @return
     */
    public int removeExact(KdbRow row) {
        int i = 0;
        while (i < curRowCount && data[i].compareTo(row) < 0) {
            i++;
        }
        if (data[i].compareTo(row) != 0) {
            return 0;
        }
        if (i < curRowCount) {
            for (int j = i; j < curRowCount - 1; j++) {
                data[j] = data[j + 1];
            }
        }
        curRowCount--;
        updateMinAndMax();
        if (curRowCount < order / 2) {
            return -1;
        } else {
            return 0;
        }
    }

    public Integer getCurrentRowCount() {
        return curRowCount;
    }

    public void updateMinAndMax() {
        if (curRowCount == 0) {
            this.maxKey = Integer.MIN_VALUE;
            this.minKey = Integer.MAX_VALUE;
        } else {
            maxKey = data[curRowCount - 1].getRowKey();
            minKey = data[0].getRowKey();
        }

    }

    public boolean isDuplicate(KdbRow row) {
        for (int i = 0; i < curRowCount; i++) {
            if (data[i].compareTo(row) == 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * 在节点插入记录，若返回 0，则插入成功，若返回 -1，则需要扩容
     * @param row
     * @return
     */
    public int add(KdbRow row) {
        if (curRowCount == 0) {
            data[0] = row;
            curRowCount++;
            updateMinAndMax();
            return 0;
        }
        if (curRowCount >= maxCount) {
            return -1;
        }
        for (int i = 0; i <= curRowCount - 1; i++) {
            if (data[i].compareTo(row) == 0) {
                throw new RuntimeException("insert duplicate row");
            }
        }
        int i = curRowCount - 1;
        while (i >= 0 && data[i].compareTo(row) > 0) {
            if (data[i].compareTo(row) == 0) {
                throw new RuntimeException("insert duplicate row");
            }
            data[i + 1] = data[i];
            i--;
        }
        data[i + 1] = row;
        curRowCount++;
        updateMinAndMax();
        return 0;
    }

    public void mergeChildren(Node leftChildren, Node rightChildren) {
        if (leftChildren.parent != rightChildren.parent) {
            throw new RuntimeException("leftChildren and rightChildren must have the same parent");
        }
    }

    public KdbRow getSplitKdbRow() {
        return data[curRowCount / 2];
    }

    public KdbRow[] getData() {
        return data;
    }

    public Integer getCurRowCount() {
        return curRowCount;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setRootNotLeaf() {
        if (isRoot) {
            this.isLeaf = false;
            this.curRowCount = 0;
        }
    }

    public Integer nodeId() {
        return nodeId;
    }

    public Node getParent() {
        return parent;
    }

    public Integer getMinKey() {
        return minKey;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public void setChildrenSep(Integer[] childrenSep) {
        this.childrenSep = childrenSep;
    }

    public void setChildrenSepCount(int childrenSepCount) {
        this.childrenSepCount = childrenSepCount;
    }

    public void setChildren(Node[] children) {
        this.children = children;
    }

    public void printChildren() {
        for (int i = 0; i < childrenSepCount; i++) {
            System.out.print("childrenSep data[" + i + "]:" + childrenSep[i] + "; ");
        }
        System.out.println();
        for (int i = 0; i < childrenSepCount + 1; i++) {
            System.out.print("childrenchildMinkey data[" + i + "]:" + children[i].getMinKey() + "; ");
        }
        System.out.println();
    }

    public void printData() {
        for (int i = 0; i < curRowCount; i++) {
            System.out.print("leaf data[" + i + "]:" + data[i].getRowKey() + "; ");
        }
        System.out.println();
    }

    public void treePrint() {
        treePrint("");
    }

    public void treePrint(String prefix) {
        System.out.print(prefix + "nodeId:" + nodeId + ", isRoot:" + isRoot + ", isLeaf:" + isLeaf + ", maxKey:" +
                maxKey + ", minKey:" + minKey);
        if (this.next != null) {
            System.out.print( ", next nodeId:" + next.getNodeId());
        } else {
            System.out.print(", next nil");
        }
        System.out.println();
        if (isLeaf) {
            // print Leaf
            System.out.println(prefix + "leaf meta curRowCount:" + curRowCount + ", maxCount:" + maxCount);
            System.out.print(prefix);
            for (int i = 0; i < curRowCount; i++) {
                System.out.print( " " + i + " leaf data[" + i + "]:" + data[i].getRowKey() + "; ");
            }
            System.out.println();
        } else {
            System.out.println(prefix + "intermediate meta childrenSepCount: " + this.childrenSepCount);
            for (int i = 0; i < childrenSepCount; i++) {
                System.out.print(prefix + "intermediate childRenSep[" + i + "]:" + childrenSep[i] + "; ");
            }
            System.out.println();
            for (int i = 0; i < childrenSepCount + 1; i++) {
                children[i].treePrint(prefix + " " + children[i].getNodeId() + "." + i + " ");
            }
        }

    }

    public Integer getNodeId() {
        return nodeId;
    }

    public Integer getStatus() {
        Integer status = 0;
        if (isRoot) {
            status = ByteUtils.setBit(status, Contants.ROOT_BIT_SHIFT, 1);
        }
        if (isLeaf) {
            status = ByteUtils.setBit(status, Contants.LEAF_BIT_SHIFT, 1);
        }
        return status;
    }


    public int getChildrenCount() {
        return childrenSepCount + 1;
    }

    public Node[] getChildren() {
        return children;
    }

    public Integer[] getChildrenSep() {
        return childrenSep;
    }

    public void updateData(KdbRow[] data, Integer curRowCount) {
        this.data = data;
        this.curRowCount = curRowCount;
        updateMinAndMax();
    }

    public int getChildrenSepCount() {
        return childrenSepCount;
    }

    public void addChildrenSepCount() {
        childrenSepCount++;
    }

    public void minusChildrenSepCount() {
        childrenSepCount--;
    }

    public boolean IsSame(Node o) {
        return nodeId.equals(o.nodeId);
    }

    public void setNext(Node next) {
        this.next = next;
    }

    public Node getNext() {
        return next;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public void updateNext(Node next) {
        this.next = next;
    }

    public static void main(String[] args) {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
        KdbRow rowSix = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 6)));
        Node node = new Node(true, true, Contants.ROOT_NODE_ID, 4);
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowThree);
        node.add(rowFour);
        node.add(rowFive);
        node.add(rowSix);
        node.treePrint();


//        node.removeBigThan(rowThree);
//        node.print();

//        node.removeSmallThan(rowThree);
//        node.print();

//        node.removeExact(rowThree);
//        node.print();

    }
}
