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
        this.root = new Node(true, true, Contants.ROOT_NODE_ID);
        this.order = order;
        this.maxNodeId = Contants.ROOT_NODE_ID;
    }

    public void updateNodeId(Integer nodeId) {
        this.maxNodeId = nodeId > maxNodeId ? nodeId : maxNodeId;
    }

    public void insert(KdbRow row) {
        // find Node2Insert
        Node curNode = root;
        while (!curNode.isLeaf()) {
            for (int i = 0; i < curNode.getChildrenCount(); i++) {
                if (row.compareTo(curNode.getData()[i]) < 0) {
                    curNode = curNode.getChildren()[i];
                    break;
                }
            }
        }
        int addRes = curNode.add(row);
        if (addRes == 0) {
            return;
        }
        if (addRes == 1) {
            // split
            Node newNode = new Node(false, false, maxNodeId + 1);
            maxNodeId++;
            newNode.updateData(curNode.getData(), curNode.getCurRowCount());
            curNode.setChildrenCount(curNode.getChildrenCount() + 1);
            curNode.getChildren()[curNode.getChildrenCount() - 1] = newNode;
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
        bPlusTree.insert(rowTwo);
        bPlusTree.insert(rowThree);
        bPlusTree.insert(rowFour);
        bPlusTree.insert(rowFive);
        bPlusTree.insert(rowSix);

        bPlusTree.print();
    }
}
