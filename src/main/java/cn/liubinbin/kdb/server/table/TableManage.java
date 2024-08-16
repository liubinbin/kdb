package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.conf.KdbConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liubinbin
 * @date 2024/8/14
 */
public class TableManage {

    private ConcurrentHashMap<String, AbstTable> tableMap;
    private TableType tableType;
    private String tableMetaPath;

    public TableManage(KdbConfig kdbConfig) {
        tableMap = new ConcurrentHashMap<String, AbstTable>(16);
        tableType = kdbConfig.getTableType();

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

    }

    public void addTable(String tableName, AbstTable table) {
        tableMap.put(tableName, table);
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

    }

    public void persist() {

    }
}
