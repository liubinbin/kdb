package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.conf.KdbConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TableManage {

    ConcurrentHashMap<String, AbstTable> tableMap;

    public TableManage(KdbConfig kdbConfig) {
        tableMap = new ConcurrentHashMap<String, AbstTable>(16);
    }

    public void init() {
        // TODO read table meta and data

        // init fake table
        ArrayList<Column> columns = new ArrayList<Column>();
        Column column1 = new Column(0, "id", ColumnType.INT, null);
        Column column2 = new Column(1, "name", ColumnType.VARCHAR, 100);
        Collections.addAll(columns, column1, column2);

        tableMap.put("test", new FakeTable("test", columns));
        tableMap.put("test1", new FakeTable("test1", columns));
    }

    public void createTable() {

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
