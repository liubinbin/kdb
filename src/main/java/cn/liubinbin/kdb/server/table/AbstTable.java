package cn.liubinbin.kdb.server.table;

import java.util.List;

/**
 * Created by liubinbin on 16/10/31.
 */
public abstract class AbstTable implements Table {

    String tableName;
    List<Column> columns;

    AbstTable(String tableName, List<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
