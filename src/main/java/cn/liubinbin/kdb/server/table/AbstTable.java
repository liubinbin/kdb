package cn.liubinbin.kdb.server.table;

import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/14.
 * 4个字节 tableNameLen
 * tableNameLen 个字节 string
 * 4个字节 tableType
 * 4个字节 column 个数
 * repeated table
     * 4个字节 columnNameLen
     * columnNameLen 个字节 columnName
     * 4个字节 columnType
     * 4个字节 columnParameter
 *
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
