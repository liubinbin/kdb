package cn.liubinbin.kdb.server.store;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liubinbin
 * 管理 page
 */
public class Store {

    private ConcurrentHashMap<Integer, Page> pageMap;

    public Store() {
        pageMap = new ConcurrentHashMap<Integer, Page>();
        // table data 的文件

    }

    public Page getPage(Integer pageId) {
        return pageMap.get(pageId);
    }

    public void putPage(Integer pageId, Page page) {
        pageMap.put(pageId, page);
    }
}
