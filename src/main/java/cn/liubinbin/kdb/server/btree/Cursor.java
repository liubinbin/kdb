package cn.liubinbin.kdb.server.btree;


import cn.liubinbin.kdb.server.entity.KdbRow;

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

    public Cursor(Node curNode) {
        this.curNode = curNode;
        this.dataIdx = 0;
    }

    public boolean hasMore() {
        if (curNode == null) {
            return false;
        } else {
            return true;
        }
    }

    public KdbRow next() {
        if (curNode != null) {
            if (dataIdx < curNode.getCurrentRowCount()) {
                return curNode.getData()[dataIdx++];
            } else {
                curNode = curNode.getNext();
                dataIdx = 0;
                return next();
            }
        } else {
            return null;
        }
    }
}
