package cn.liubinbin.kdb.server.table;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
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

    private String tableName;
    private List<Column> columns;
    private TableType tableType;

    AbstTable(String tableName, List<Column> columns, TableType tableType) {
        this.tableName = tableName;
        this.columns = columns;
        this.tableType = tableType;
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

    public void writeTo(RandomAccessFile raf) throws IOException {
        // tableName
        raf.writeInt(this.tableName.getBytes().length);
        raf.write(this.tableName.getBytes());

        // tableType
        raf.writeInt(this.tableType.getType());

        // columnLen
        raf.writeInt(this.columns.size());

        // columnData
        for (int i = 0; i < this.columns.size(); i++) {
            Column curColumn = this.columns.get(i);
            raf.writeInt(curColumn.getColumnName().getBytes().length);
            raf.write(curColumn.getColumnName().getBytes());
            raf.writeInt(curColumn.getColumnType().getColumnTypeInt());
            raf.writeInt(curColumn.getColumnParameter());
        }
    }

    public static AbstTable readFrom(RandomAccessFile raf) throws IOException {
        AbstTable abstTable = null;

        // tableName
        int tableNameLen = raf.readInt();
        byte[] tableNameBytes = new byte[tableNameLen];
        raf.read(tableNameBytes);
        String dbName = new String(tableNameBytes);

        // tableType
        int tableTypeInt = raf.readInt();
        TableType tableType = TableType.getTableType(tableTypeInt);

        // columnLen
        int columnLen = raf.readInt();

        // columnData
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnLen; i++) {
            int CurColNameLen = raf.readInt();
            byte[] curColNameBytes = new byte[CurColNameLen];
            raf.read(curColNameBytes);
            String curColName = new String(curColNameBytes);
            ColumnType curColType = ColumnType.getColumnType(raf.readInt());
            Integer curColParameter = raf.readInt();
            Column curColumn = new Column(i, curColName, curColType, curColParameter);
            columns.add(curColumn);
        }
        if (tableType == TableType.Btree) {
            abstTable = new BtreeTable(dbName, columns);
        } else if (tableType == TableType.Fake) {
            abstTable = new FakeTable(dbName, columns);
        }

        return abstTable;
    }

    public void print() {
        System.out.println(" AbstTable print");
    }


    @Override
    public String toString() {
        return "AbstTable{" +
                "tableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", tableType=" + tableType +
                '}';
    }
}
