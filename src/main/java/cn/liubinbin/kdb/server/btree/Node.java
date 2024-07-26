package cn.liubinbin.kdb.server.btree;

public class Node {

    private boolean isRoot;
    private boolean isLeaf;
    private Integer nodeId;

    public Node(boolean isRoot, boolean isLeaf, Integer nodeId) {
        this.isRoot = isRoot;
        this.isLeaf = isLeaf;
        this.nodeId = nodeId;
    }
}
