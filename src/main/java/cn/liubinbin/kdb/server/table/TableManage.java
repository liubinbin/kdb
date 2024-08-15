package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.conf.KdbConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TableManage {

    ConcurrentHashMap<String, AbstTable> tableMap;

    public TableManage(KdbConfig kdbConfig) {
        tableMap = new ConcurrentHashMap<String, AbstTable>();
    }

    public void init() {
        // TODO read table meta and data
        tableMap.put("test", new FakeTable("test"));
        tableMap.put("test1", new FakeTable("test"));
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

    public void close() {

    }

    public void persist() {

    }
}
