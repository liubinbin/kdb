package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.TableType;
import cn.liubinbin.kdb.utils.Contants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
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
    private List<Column> columns;
    private TableType tableType;

    public TableStore(String tableName, KdbConfig kdbConfig, List<Column> columns, TableType tableType) {
        this.tableName = tableName;
        this.pageMap = new ConcurrentHashMap<Integer, Page>();
        this.kdbConfig = kdbConfig;
        this.tableDataBackupFilePath = kdbConfig.getTableRootPath() + Contants.FILE_SEPARATOR + tableName + kdbConfig.getBackupFileExtension();
        this.tableDataFilePath = kdbConfig.getTableRootPath() + Contants.FILE_SEPARATOR + tableName + kdbConfig.getDataFileExtension();
    }

    public void init() {
        int offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableDataBackupFilePath), "rw")) {
            while (offset < raf.length()) {
                Node node = new Node(false, false,null, kdbConfig.getBtreeOrder());
                Page page = new Page(node, tableName, columns);
                page.readFrom(raf, offset);
                node = page.exactFromBytes(kdbConfig.getBtreeOrder());
                offset += page.getPageSize();
                pageMap.put(node.getNodeId(), page);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() throws IOException {
        int offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableDataBackupFilePath), "rw")) {
            for (Page page : pageMap.values()) {
                page.writeTo(raf, offset);
                offset += page.getPageSize();
            }
        }
    }

    public Page getPage(Integer pageId) {
        return pageMap.get(pageId);
    }

    public void putPage(Integer pageId, Page page) {
        pageMap.put(pageId, page);
    }
}
