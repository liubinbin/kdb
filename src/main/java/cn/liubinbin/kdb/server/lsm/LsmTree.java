package cn.liubinbin.kdb.server.lsm;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.executor.Engine;

import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author liubinbin
 */
public class LsmTree extends Engine {

    ConcurrentSkipListMap<Integer, KdbRow> tree;

    public LsmTree() {
        super();
    }
}
