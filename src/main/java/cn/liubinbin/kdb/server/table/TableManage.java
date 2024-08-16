package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.utils.Contants;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.checkerframework.checker.units.qual.K;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liubinbin
 * @date 2024/8/14
 * file format
 * db meta
 *  dnName.len
 *  dbName bytes
 *  table.len
 * table meta
 *  ref to tableMeta
 */
public class TableManage {

    private ConcurrentHashMap<String, AbstTable> tableMap;
    private TableType tableType;
    private String tableMetaFullPath;
    private String tableMetaFullBackupPath;

    public TableManage(KdbConfig kdbConfig) {
        this.tableMap = new ConcurrentHashMap<String, AbstTable>(16);
        this.tableType = kdbConfig.getTableType();
        this.tableMetaFullPath = kdbConfig.getMetaFullPath();
        this.tableMetaFullBackupPath = kdbConfig.getMetaFullPath() + kdbConfig.getBackupFileExtension();
        System.out.println("kdbConfig tableMetaFullPath " + tableMetaFullPath);
        System.out.println("kdbConfig tableMetaFullBackupPath "  + tableMetaFullBackupPath);
    }

    public void init() {
        // TODO read table meta and data

        // init fake table
        ArrayList<Column> columns = new ArrayList<Column>();
        Column column1 = new Column(0, "id", ColumnType.INTEGER, null);
        Column column2 = new Column(1, "name", ColumnType.VARCHAR, 100);
        Collections.addAll(columns, column1, column2);

        tableMap.put("test", new FakeTable("test", columns));
        tableMap.put("test1", new FakeTable("test1", columns));
    }

    public void createTable(String tableName, List<Column> columns) {
        // 更新内存
        if (tableMap.containsKey(tableName)) {
            throw new RuntimeException("table already exists");
        }
        if (tableType == TableType.Btree) {
            System.out.println("create btree table");
            tableMap.put(tableName, new BtreeTable(tableName, columns));
        } else {
            System.out.println("create fake table");
            tableMap.put(tableName, new FakeTable(tableName, columns));
        }
        // 保存文件
        writeTo();
    }

    public AbstTable getTable(String tableName) {
        return tableMap.get(tableName);
    }

    public List<String> ListTableName(){
        return new ArrayList<>(tableMap.keySet());
    }

    public List<Column> describeTable(String tableName){
        return tableMap.get(tableName).getColumns();
    }

    public void close() {
        writeTo();
    }


    public void readFrom() {
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableMetaFullBackupPath), "r")) {
            raf.seek(0);

            // 读取数据
            int dbNameLen = raf.readInt(); // 读取整数
            byte[] dbNameBytes = new byte[dbNameLen];
            raf.read(dbNameBytes);
            String dbName = new String(dbNameBytes);
            int tableLen = raf.readInt();

            // 输出结果
            System.out.println("dbNameLength: " + dbNameLen);
            System.out.println("dbName: " + dbName);
            System.out.println("tableLen " + tableLen);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeTo() {
        try (RandomAccessFile raf = new RandomAccessFile(new File(tableMetaFullBackupPath), "rw")) {
            // db meta
            raf.writeInt(Contants.DEFAULT_KDB_DATABASE_NAME.getBytes().length);
            raf.write(Contants.DEFAULT_KDB_DATABASE_NAME.getBytes());
            raf.writeInt(this.tableMap.size());

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws ConfigurationException, IOException {
        KdbConfig kdbConfig = new KdbConfig();
        TableManage tableManage = new TableManage(kdbConfig);

        tableManage.init();
        tableManage.writeTo();
        tableManage.readFrom();
    }
}
