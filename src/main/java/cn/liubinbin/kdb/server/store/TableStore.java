package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.TableType;
import cn.liubinbin.kdb.utils.Contants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
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
    private Integer order;

    public TableStore(String tableName, String tableDataFilePath, String tableDataBackupFilePath, List<Column> columns, TableType tableType, Integer order) {
        this.tableName = tableName;
        this.pageMap = new ConcurrentHashMap<Integer, Page>();
        this.tableDataFilePath = tableDataFilePath;
        this.tableDataBackupFilePath = tableDataBackupFilePath;
        this.columns = columns;
        this.tableType = tableType;
        this.order = order;
    }

    public TableStore(String tableName, KdbConfig kdbConfig, List<Column> columns, TableType tableType) {
        String tableDataFilePath = kdbConfig.getTableRootPath() + Contants.FILE_SEPARATOR + tableName + kdbConfig.getDataFileExtension();
        String tableDataBackupFilePath = tableDataFilePath + kdbConfig.getBackupFileExtension();
        this.tableName = tableName;
        this.pageMap = new ConcurrentHashMap<Integer, Page>();
        this.tableDataFilePath = tableDataFilePath;
        this.tableDataBackupFilePath = tableDataBackupFilePath;
        this.columns = columns;
        this.tableType = tableType;
        this.order = kdbConfig.getBtreeOrder();
    }

    public void readDataFile() {
        int offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableDataFilePath), "rw")) {
            while (offset < raf.length()) {
                Node node = new Node(false, false,null, order);
                Page page = new Page(node, columns);
                page.readFrom(raf, offset);
                node = page.exactFromBytes(order);
                offset += page.getPageSize();
                pageMap.put(node.getNodeId(), page);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<Integer, Node> getNodeMap() {
        HashMap<Integer, Node> nodeMap = new HashMap<Integer, Node>();
        for (Page page : pageMap.values()) {
            nodeMap.put(page.getNode().getNodeId(), page.getNode());
        }
        return nodeMap;
    }

    public void close() throws IOException {
        int offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableDataBackupFilePath), "rw")) {
            for (Page page : pageMap.values()) {
                if (page.getNode().isRoot() && page.getNode().isLeaf() && page.getNode().getCurRowCount() < 1) {
                    continue;
                }
                page.writeTo(raf, offset);
                offset += page.getPageSize();
            }
        }
        reNameBackupTableData();
    }

    private void reNameBackupTableData(){
        File file = new File(tableDataFilePath);
        File backFile = new File(tableDataBackupFilePath);
        if (file.exists()) {
            file.delete();
        }
        boolean renameIfSucc = backFile.renameTo(file);
        if (!renameIfSucc) {
            throw new RuntimeException("rename table data file failed");
        }
    }

    public Page getPage(Integer pageId) {
        return pageMap.get(pageId);
    }

    public void addNode(Node node) {
        Page page = new Page(node, columns);
        putPage(node.getNodeId(), page);
    }

    public void deleteNode(Node node) {
        pageMap.remove(node.getNodeId());
    }

    public void putPage(Integer pageId, Page page) {
        pageMap.put(pageId, page);
    }

}
