package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.utils.Contants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liubinbin
 * 管理 page
 */
public class TableStore {

    private ConcurrentHashMap<Integer, Page> pageMap;
    private KdbConfig kdbConfig;
    private String tableName;
    private String tableDataBackupFilePath;
    private String tableDataFilePath;

    public TableStore(String tableName, KdbConfig kdbConfig) {
        this.tableName = tableName;
        this.pageMap = new ConcurrentHashMap<Integer, Page>();
        this.kdbConfig = kdbConfig;
        this.tableDataBackupFilePath = kdbConfig.getTableRootPath() + Contants.FILE_SEPARATOR + tableName + kdbConfig.getBackupFileExtension();
        this.tableDataFilePath = kdbConfig.getTableRootPath() + Contants.FILE_SEPARATOR + tableName + kdbConfig.getDataFileExtension();
    }

    public void init() {

    }

    public void close() throws IOException {
        int offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableDataBackupFilePath), "rw")) {
            for (Page page : pageMap.values()) {
                page.writeTo(raf, offset);
                offset += page.getPageSize();
            }
        }
        System.out.println("   offset  " + offset);
    }

    public Page getPage(Integer pageId) {
        return pageMap.get(pageId);
    }

    public void putPage(Integer pageId, Page page) {
        pageMap.put(pageId, page);
    }
}
