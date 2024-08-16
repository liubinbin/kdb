package cn.liubinbin.kdb.server.btree;

import cn.liubinbin.kdb.server.entity.KdbRow;

/**
 * @author liubinbin
 * B树节点实现
 */
public class Node {

    private boolean isRoot;
    private boolean isLeaf;
    private Integer nodeId;
    private KdbRow[] data;
    private Integer rowCount;
    private Node[] children;
    private Integer max;
    private Integer min;

    Node(boolean isRoot, boolean isLeaf, Integer nodeId) {
        this.isRoot = isRoot;
        this.isLeaf = isLeaf;
        this.nodeId = nodeId;
    }

    public void add(KdbRow row) {
        if (rowCount == 0) {
            data[0] = row;
            rowCount++;
            return;
        }
        if (rowCount == max) {
            return;
        }
        int i = rowCount - 1;
        while (i >= 0 && data[i].compareTo(row) > 0) {
            data[i + 1] = data[i];
            i--;
        }
        data[i + 1] = row;
    }

    public KdbRow[] getData() {
        return data;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public Integer nodeId() {
        return nodeId;
    }

    public void print() {
        System.out.println("nodeId:" + nodeId);
        System.out.println("isRoot:" + isRoot);
        System.out.println("isLeaf:" + isLeaf);
        System.out.println("rowCount:" + rowCount);
        System.out.println("max:" + max);
        System.out.println("min:" + min);
        System.out.println("data:");
    }

    public static void main(String[] args) {

    }
}
