package cn.liubinbin.kdb.server.table;

import java.util.concurrent.ConcurrentHashMap;

public class TableManage {

    ConcurrentHashMap<String, AbstTable> tableMap;

    public TableManage() {
        tableMap = new ConcurrentHashMap<String, AbstTable>();
    }

    public void init() {
        // TODO read talble meta and data
        tableMap.put("test", new FakeTable("test"));
    }

    public void addTable(String tableName, AbstTable table) {
        tableMap.put(tableName, table);
    }

    public AbstTable getTable(String tableName){
        return tableMap.get(tableName);
    }
}
