package cn.liubinbin.kdb.server.store;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.table.TableManage;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liubinbin
 * 管理 TableStore
 */
public class StoreManage {

    private ConcurrentHashMap<String, TableStore> tableStoreMap;
    private KdbConfig kdbConfig;
    private TableManage tableManage;

    public StoreManage(KdbConfig kdbConfig, TableManage tableManage) {
        this.tableStoreMap = new ConcurrentHashMap<String, TableStore>();
        this.kdbConfig = kdbConfig;
        this.tableManage = tableManage;
    }

    public void init() {
        List<String> tableNameList = tableManage.ListTableName();
        for (String tableName : tableNameList) {
            TableStore tableStore = new TableStore(tableName, kdbConfig, tableManage.getTable(tableName).getColumns(),
                    tableManage.getTable(tableName).getTableType());
            tableStore.init();
            tableStoreMap.put(tableName, tableStore);
        }
    }

    public void close() throws IOException {
        List<String> tableNameList = tableManage.ListTableName();
        for (String tableName : tableNameList) {
            tableStoreMap.get(tableName).close();
        }
    }

}
