package cn.liubinbin.kdb.server.btree;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.executor.Engine;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;

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
        return maxNodeId++;
    }

    public void insert(KdbRow rowToInsert) {
        // find Node2Insert
        Node curNode = root;
        Node tempNode = null;
        while (!curNode.isLeaf()) {
            tempNode = curNode.getChildren()[0];
            for (int i = 0; i < curNode.getChildrenCount(); i++) {
                if (rowToInsert.compareTo(curNode.getData()[i]) >= 0) {
                    break;
                }
                tempNode = curNode.getChildren()[i+1];
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
        KdbRow rowSplitKey = curNode.getSplitKdbRow();
        KdbRow[] curNodeData = curNode.getData();
        for (int i = 0; i < curNode.getCurRowCount(); i++) {
            if (curNodeData[i].compareTo(rowSplitKey) < 0) {
                leftNode.add(curNodeData[i]);
            } else {
                rightNode.add(curNodeData[i]);
            }
        }

        // 添加 row
        leftNode.add(rowToInsert);

        // 修改 split
        if (curNode.isRoot()) {
            curNode.splitChildren(leftNode, rightNode, rowSplitKey.getRowKey());
            curNode.setRootNotLeaf();
        } else {
            Node parent = curNode.getParent();
            parent.splitChildren(leftNode, rightNode, rowSplitKey.getRowKey());
        }

    }

    public void delete(KdbRow row) {

    }

    public void rangeScan(Integer lowerBound, Integer upperBound) {
        // find in the most left node
    }

    public void print() {
        root.print();
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public static void main(String[] args) {
        BPlusTree bPlusTree = new BPlusTree(3);

        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
        KdbRow rowSix = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 6)));
        bPlusTree.insert(rowOne);
        System.out.println("after insert rowOne ");
        bPlusTree.print();

        bPlusTree.insert(rowTwo);
        System.out.println("after insert rowTwo ");
        bPlusTree.print();

        bPlusTree.insert(rowThree);
        System.out.println("after insert rowThree ");
        bPlusTree.print();

        bPlusTree.insert(rowFour);
        System.out.println("after insert rowFour ");
        bPlusTree.print();

        bPlusTree.insert(rowFive);
        System.out.println("after insert rowFive ");
        bPlusTree.print();

        bPlusTree.insert(rowSix);
        System.out.println("after insert rowSix ");
        bPlusTree.print();

    }
}
