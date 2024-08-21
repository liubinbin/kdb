package cn.liubinbin.kdb.server.btree;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.Engine;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author liubinbin
 */
public class BPlusTree extends Engine {

    private Node root;
    private Integer order;
    private Integer maxNodeId;

    BPlusTree(Integer order) {
        this.order = order;
        this.root = new Node(true, true, Contants.ROOT_NODE_ID, order);
        this.maxNodeId = Contants.ROOT_NODE_ID;
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

        // add and split by the result
        int addRes = curNode.removeExact(rowToDelete);
        if (addRes == 0) {
            return;
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

        splitInternalChildren(curNode);
    }

    public void rangeScan(Integer lowerBound, Integer upperBound) {
        // find in the most left node
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
        BPlusTree bPlusTree = new BPlusTree(3);

//        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
//        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
//        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
//        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
//        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
//        KdbRow rowSix = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 6)));

        for(int i = 1; i <= 7; i++) {
            KdbRow curRow = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, i)));
            System.out.println("--- before insert row " + i + " ---");
            bPlusTree.insert(curRow);
            System.out.println("--- after insert row " + i + " ---");
            bPlusTree.print();
        }

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
