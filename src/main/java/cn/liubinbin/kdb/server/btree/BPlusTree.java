package cn.liubinbin.kdb.server.btree;

import cn.liubinbin.kdb.server.Engine;

public class BPlusTree extends Engine{
    
    private Node root;
    private Integer order;

    BPlusTree(Integer order) {
        root = new Node();
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public void insert(){


    }
    
    public void delete() {

    }

    public void rangeScan() {

    }
    
}