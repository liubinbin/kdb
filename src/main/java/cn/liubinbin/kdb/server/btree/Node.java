package cn.liubinbin.kdb.server.btree;

public class Node {

    private boolean isRoot;
    private boolean isLeaf;
    private Integer nodeId;

    Node() {
    }

    Node(boolean isRoot, boolean isLeaf, Integer nodeId) {
        this.isRoot = isRoot;
        this.isLeaf = isLeaf;
        this.nodeId = nodeId;
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
}
