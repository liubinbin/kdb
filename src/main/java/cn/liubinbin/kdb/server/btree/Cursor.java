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

    /**
     * 若返回 false，则没有数据了。
     * @return
     */
    public boolean hasMore() {
        if (curNode == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 返回的数据可能是 null，在上层需要处理
     * @return
     */
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
