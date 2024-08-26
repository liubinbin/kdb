package cn.liubinbin.kdb.server.btree;


/**
 * @author liubinbin
 * @date 2024/08/26
 */
public class Cursor {

    Node curNode;
    Integer dataIdx;

    public Cursor(Node curNode, Integer dataIdx) {
        this.curNode = curNode;
        this.dataIdx = dataIdx;
    }
}
