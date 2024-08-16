package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.server.btree.Node;

/**
 * @author liubinbin
 * @date 2024/08/16
 *
 * 一块连续内存，数据来自 Node，写入到文件中。
 * 关联一个 Node
 */
public class Page {

    public static final int PAGE_SIZE = 16 * 1024; // 16KB
    private byte[] page;
    private Node node;
    private int offset;

    public void writeTo(Node node) {

    }

    public void readFrom() {

    }
}
