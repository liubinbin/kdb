package cn.liubinbin.kdb.server.btree;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.Engine;
import cn.liubinbin.kdb.server.store.TableStore;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author liubinbin
 */
public class BPlusTree extends Engine {

    private Node root;
    private final Integer order;
    private Integer maxNodeId;
    private ReadWriteLock lock;
    private TableStore tableStore;

    public BPlusTree(Integer order, TableStore tableStore) {
        this.order = order;
        this.root = new Node(true, true, Contants.ROOT_NODE_ID, order);
        this.maxNodeId = Contants.ROOT_NODE_ID;
        this.tableStore = tableStore;
        addNode(this.root);
    }

    public void addNode(Node node) {
        if (this.tableStore != null) {
            this.tableStore.addNode(node);
        }
    }

    public void deleteNode(Node node) {
        if (this.tableStore != null) {
            this.tableStore.deleteNode(node);
        }
    }

    public void updateNodeId(Integer nodeId) {
        this.maxNodeId = nodeId > maxNodeId ? nodeId : maxNodeId;
    }

    public synchronized Integer getAndAddNodeId() {
        return ++maxNodeId;
    }

    public KdbRow[] mergeKdbRowAndSort(KdbRow[] left, KdbRow right) {
        KdbRow[] result = new KdbRow[left.length + 1];
        System.arraycopy(left, 0, result, 0, left.length);
        result[left.length] = right;
        Arrays.sort(result);
        return result;
    }

    public void delete(KdbRow rowToDelete) {
        // find Node2Insert
        Node curNode = root;
        Node tempNode = null;
        while (!curNode.isLeaf()) {
            tempNode = curNode.getChildren()[0];
            for (int i = 0; i < curNode.getChildrenCount() - 1; i++) {
                if (rowToDelete.getRowKey() > curNode.getChildrenSep()[i]) {
                    tempNode = curNode.getChildren()[i+1];
                } else {
                    break;
                }
            }
            curNode = tempNode;
        }

        System.out.println("curNode " + curNode.getNodeId());
        // add and split by the result
        int addRes = curNode.removeExact(rowToDelete);
        if (addRes == 0) {
            return;
        }

        System.out.println("should merge");
        Node parent = curNode.getParent();
        mergeLeafChildren(curNode, parent);
    }

    private void mergeLeafChildren(Node curNode, Node parent) {
        if (parent.isRoot() && parent.getChildrenCount() <= 1) {
            parent.setLeaf(true);
        }
        System.out.println("do some merge for leaf children");
        int childIdxInParent = 0;
        for (childIdxInParent = 0; childIdxInParent < parent.getChildrenCount(); childIdxInParent++) {
            if (parent.getChildren()[childIdxInParent] == curNode) {
                break;
            }
        }
        Node leftNode = null;
        Node rightNode = null;
        if (childIdxInParent == 0) {
            leftNode = parent.getChildren()[0];
            rightNode = parent.getChildren()[1];
        } else if (childIdxInParent == parent.getChildrenCount() - 1) {
            leftNode = parent.getChildren()[childIdxInParent - 1];
            rightNode = parent.getChildren()[childIdxInParent];
            childIdxInParent--;
        } else {
            leftNode = parent.getChildren()[childIdxInParent];
            rightNode = parent.getChildren()[childIdxInParent + 1];
        }

        Node newNode = new Node(false, true, getAndAddNodeId(), order);
        for (int i = 0; i< leftNode.getCurRowCount(); i++) {
            newNode.add(leftNode.getData()[i]);
        }
        for (int i = 0; i< rightNode.getCurRowCount(); i++) {
            newNode.add(rightNode.getData()[i]);
        }
        newNode.setParent(parent);
        newNode.setNext(rightNode.getNext());

        // 移动 childrenSep 和 child
        parent.getChildren()[childIdxInParent] = newNode;
        for(int i = childIdxInParent + 1; i < parent.getChildrenCount() - 1; i++){
            parent.getChildren()[i] = parent.getChildren()[i + 1];
        }
        for(int i = childIdxInParent; i < parent.getChildrenSepCount() - 1; i++){
            parent.getChildrenSep()[i] = parent.getChildrenSep()[i + 1];
        }
        parent.minusChildrenSepCount();

        if (parent.isRoot()) {
            return;
        } else {
            Node newParent = parent.getParent();
            mergeLeafChildren(parent, newParent);
        }
    }

    public void insert(KdbRow rowToInsert) {
        // find Node2Insert
        Node curNode = root;
        Node tempNode = null;
        while (!curNode.isLeaf()) {
            tempNode = curNode.getChildren()[0];
            for (int i = 0; i < curNode.getChildrenCount() - 1; i++) {
                if (rowToInsert.getRowKey() > curNode.getChildrenSep()[i]) {
                    tempNode = curNode.getChildren()[i+1];
                } else {
                    break;
                }
            }
            curNode = tempNode;
        }

        // add and split by the result
        int addRes = curNode.add(rowToInsert);
        if (addRes == 0) {
            return;
        }
        // addRes != 0 means should split
        // new data
        Node leftNode = new Node(false, true, getAndAddNodeId(), order);
        Node rightNode = new Node(false, true, getAndAddNodeId(), order);
        leftNode.setNext(rightNode);
        // 获取 splitkey
        KdbRow[] curNodeData = mergeKdbRowAndSort(curNode.getData(), rowToInsert);
        Integer splitRowKey = curNodeData[(curNodeData.length / 2) + 1].getRowKey();
        for (int i = 0; i < curNodeData.length; i++) {
            if (i <= curNodeData.length / 2) {
                leftNode.add(curNodeData[i]);
            } else {
                rightNode.add(curNodeData[i]);
            }
        }

        // 修改 split
        if (curNode.isRoot()) {
            splitLeafChildren(curNode, leftNode, rightNode, splitRowKey);
            curNode.setRootNotLeaf();
        } else {
            Node parent = curNode.getParent();
            splitLeafChildren(parent, leftNode, rightNode, splitRowKey);
        }
    }

    //
    public void splitInternalChildren(Node curNode){
        if (curNode.getChildrenSepCount() <= order - 1) {
            return;
        }
        // child 分为两个部分
        Integer splitRowKey = curNode.getChildrenSep()[(curNode.getChildrenSepCount() / 2)];
        Integer[] leftNodeChildrenSep = new Integer[order];
        Integer leftNodeChildIdx = 0;
        Node[] leftNodeChildren = new Node[order + 1];

        Integer curNodeChildSep = 0;

        Integer[] rightNodeChildrenSep = new Integer[order];
        Integer rightNodeChildIdx = 0;
        Node[] rightNodeChildren = new Node[order + 1];

        leftNodeChildren[leftNodeChildIdx] = curNode.getChildren()[0];
        for (int i = 0; i < curNode.getChildrenSepCount(); i++) {
            if (i < curNode.getChildrenSepCount() / 2) {
                leftNodeChildrenSep[leftNodeChildIdx] = curNode.getChildrenSep()[i];
                leftNodeChildren[leftNodeChildIdx+1] = curNode.getChildren()[i+1];
                leftNodeChildIdx++;
            } else if (i == curNode.getChildrenSepCount() / 2) {
                curNodeChildSep = curNode.getChildrenSep()[i];
                rightNodeChildren[rightNodeChildIdx] = curNode.getChildren()[i+1];
            } else {
                rightNodeChildrenSep[rightNodeChildIdx] = curNode.getChildrenSep()[i];
                rightNodeChildren[rightNodeChildIdx+1] = curNode.getChildren()[i+1];
                rightNodeChildIdx++;
            }
        }

        Node leftChildren = new Node(false, false, getAndAddNodeId(), order);
        Node rightChildren = new Node(false, false, getAndAddNodeId(), order);

        leftChildren.setParent(curNode);
        leftChildren.setChildren(leftNodeChildren);
        leftChildren.setChildrenSep(leftNodeChildrenSep);
        leftChildren.setChildrenSepCount(leftNodeChildIdx);

        rightChildren.setParent(curNode);
        rightChildren.setChildren(rightNodeChildren);
        rightChildren.setChildrenSep(rightNodeChildrenSep);
        rightChildren.setChildrenSepCount(rightNodeChildIdx);

        Node leftChildInLeftChild = leftChildren.getChildren()[leftChildren.getChildrenCount() - 1];
        Node rightChildInRightChild = rightChildren.getChildren()[0];
        leftChildInLeftChild.setNext(rightChildInRightChild);

//        System.out.println("print two child start");
//        leftChildren.printChildren();
//        rightChildren.printChildren();
//        System.out.println("print two child end");

        // 更新父节点
        if (curNode.isRoot()) {
            Node newRoot = new Node(true, false, getAndAddNodeId(), order);
            updateChild(newRoot, curNodeChildSep, leftChildren, rightChildren);
            this.root = newRoot;
        } else {
            Node parent = curNode.getParent();
            updateChild(parent, curNodeChildSep, leftChildren, rightChildren);
            splitInternalChildren(parent);
        }
    }

    public void updateChild(Node curNode, Integer newChildrenSep, Node leftChildren, Node rightChildren) {
        Integer oriChildSep = leftChildren.getMinKey();
        int idxToInsert = 0;
        if (curNode.getChildrenSepCount() != 0) {
            for (int i = curNode.getChildrenSepCount() - 1; i >= 0; i--) {
                if (curNode.getChildrenSep()[i] > oriChildSep) {
                    curNode.getChildrenSep()[i + 1] = curNode.getChildrenSep()[i];
                    curNode.getChildren()[i + 2] = curNode.getChildren()[i + 1];
                } else {
                    idxToInsert = i + 1;
                    break;
                }
            }
        }

        leftChildren.setParent(curNode);
        rightChildren.setParent(curNode);

        curNode.getChildren()[idxToInsert] = leftChildren;
        curNode.getChildren()[idxToInsert + 1] = rightChildren;
        curNode.getChildrenSep()[idxToInsert] = newChildrenSep;
        curNode.addChildrenSepCount();

        if (idxToInsert > 0) {
            curNode.getChildren()[idxToInsert -1].setNext(leftChildren);
        }
    }

    public void splitLeafChildren(Node curNode, Node leftChildren, Node rightChildren, Integer childrenSepKey) {
        if (!rightChildren.getMinKey().equals(childrenSepKey)) {
            throw new RuntimeException("childrenSepKey is equal to minKey");
        }

        Integer oriChildSep = leftChildren.getMinKey();
        int idxToInsert = 0;
        if (curNode.getChildrenSepCount() != 0) {
            for (int i = curNode.getChildrenSepCount() - 1; i >= 0; i--) {
                if (curNode.getChildrenSep()[i] > oriChildSep) {
                    curNode.getChildrenSep()[i + 1] = curNode.getChildrenSep()[i];
                    curNode.getChildren()[i + 2] = curNode.getChildren()[i + 1];
                } else {
                    idxToInsert = i + 1;
                    break;
                }
            }
        }

        leftChildren.setParent(curNode);
        rightChildren.setParent(curNode);

        curNode.getChildren()[idxToInsert] = leftChildren;
        curNode.getChildren()[idxToInsert + 1] = rightChildren;
        curNode.getChildrenSep()[idxToInsert] = childrenSepKey;

        curNode.addChildrenSepCount();

        if (idxToInsert > 0) {
            curNode.getChildren()[idxToInsert -1].setNext(leftChildren);
        }

        splitInternalChildren(curNode);
    }

    public List<KdbRow> rangeScan() {
        return this.rangeScan(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Node getRangeScanStartNode(Integer lowerBound) {
        Node curNode = root;
        Node tempNode = null;
        while (!curNode.isLeaf()) {
            tempNode = curNode.getChildren()[0];
            for (int i = 0; i < curNode.getChildrenCount() - 1; i++) {
                if (lowerBound >= curNode.getChildrenSep()[i]) {
                    tempNode = curNode.getChildren()[i+1];
                } else {
                    break;
                }
            }
            curNode = tempNode;
        }
        return curNode;
    }

    /**
     * range 搜索
     * [lowerBound, upperBound]
     * @param lowerBound
     * @param upperBound
     * @return
     */
    public List<KdbRow> rangeScan(Integer lowerBound, Integer upperBound) {
        // find in the most left node
        List<KdbRow> rows = new ArrayList<>();
        Node curNode = root;
        Node tempNode = null;
        while (!curNode.isLeaf()) {
            tempNode = curNode.getChildren()[0];
            for (int i = 0; i < curNode.getChildrenCount() - 1; i++) {
                if (lowerBound >= curNode.getChildrenSep()[i]) {
                    tempNode = curNode.getChildren()[i+1];
                } else {
                    break;
                }
            }
            curNode = tempNode;
        }
        while (curNode != null) {
            for (int i = 0; i < curNode.getCurRowCount(); i++) {
                KdbRow row = curNode.getData()[i];
                if (row.getRowKey() >= lowerBound && row.getRowKey() <= upperBound) {
                    rows.add(row);
                } else if (row.getRowKey() > upperBound) {
                    break;
                }
            }
            curNode = curNode.getNext();
        }
        return rows;
    }

    public void print() {
        root.treePrint();
        System.out.println("\n");
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public static void main(String[] args) {
        BPlusTree bPlusTree = new BPlusTree(3, null);

        for(int i = 1; i <= 6; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            System.out.println("--- before insert row " + i + " ---");
            bPlusTree.insert(curRow);
            System.out.println("--- after insert row " + i + " ---");
            bPlusTree.print();
        }

//        for (int i = -1; i <= 6; i++) {
//            System.out.println(" i  " + i);
//            List<KdbRow> rows = bPlusTree.rangeScan(i, 100);
//        }
        List<KdbRow> rows = bPlusTree.rangeScan(4, 5);
        System.out.println("rows size " + rows.size());
        for (KdbRow row : rows) {
            System.out.print(row.getRowKey() + ", ");
        }

//        for(int i = 0; i <= 6; i++) {
//            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
//            System.out.println("--- before delete row " + i + " ---");
//            bPlusTree.delete(curRow);
//            System.out.println("--- after delete row " + i + " ---");
//            bPlusTree.print();
//        }
//
//        KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
//        System.out.println("--- before insert row 1  ---");
//        bPlusTree.insert(curRow);
//        System.out.println("--- after insert row 1 ---");
//        bPlusTree.print();

//        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
//        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
//        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
//        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
//        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
//        KdbRow rowSix = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 6)));
//        bPlusTree.insert(rowOne);
//        System.out.println("--- after insert rowOne ---");
//        bPlusTree.print();
//
//        bPlusTree.insert(rowTwo);
//        System.out.println("--- after insert rowTwo --- ");
//        bPlusTree.print();
//
//        bPlusTree.insert(rowThree);
//        System.out.println("--- after insert rowThree --- ");
//        bPlusTree.print();
//
//        bPlusTree.insert(rowFour);
//        System.out.println("--- after insert rowFour --- ");
//        bPlusTree.print();
//
//        bPlusTree.insert(rowFive);
//        System.out.println("--- after insert rowFive --- ");
//        bPlusTree.print();
//
//        System.out.println("--- before insert rowSix --- ");
//        bPlusTree.insert(rowSix);
//        System.out.println("--- after insert rowSix --- ");
//        bPlusTree.print();

    }
}
