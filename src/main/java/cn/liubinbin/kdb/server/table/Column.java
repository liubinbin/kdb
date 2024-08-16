package cn.liubinbin.kdb.server.table;

/**
 * @author liubinbin
 * @info Created by liubinbin on 16/3/16.
 */
public class Column {

    private int idx;
    private String columnName;
    private ColumnType columnType;
    private Integer columnParameter;

    public Column(int idx, String columnName, ColumnType columnType, Integer columnParameter) {
        this.idx = idx;
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnParameter = columnParameter;
    }

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

    public Integer getColumnParameter() {
        return columnParameter;
    }

    public void setColumnParameter(Integer columnParameter) {
        this.columnParameter = columnParameter;
    }

    @Override
    public String toString() {
        return "Column{" +
                "idx=" + idx +
                ", columnName='" + columnName + '\'' +
                ", columnType=" + columnType +
                ", columnParameter=" + columnParameter +
                '}';
    }
}
