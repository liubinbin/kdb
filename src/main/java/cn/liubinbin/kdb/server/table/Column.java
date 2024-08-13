package cn.liubinbin.kdb.server.table;

/**
 * Created by liubinbin on 16/3/16.
 */
public class Column {

    private int idx;
    private String columnName;
    private ColumnType columnType;

    public int getIdx() {
        return idx;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }
}
