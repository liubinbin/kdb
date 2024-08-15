package cn.liubinbin.kdb.server.table;

/**
 * @author liubinbin
 * @info Created by liubinbin on 16/10/30.
 */
public enum ColumnType {

    INT(0),
    VARCHAR(1);

    private final int columnType;

    ColumnType(int columnType) {
        this.columnType = columnType;
    }

    public int getColumnType() {
        return columnType;
    }

    public static ColumnType getColumnType(int value) {
        switch (value) {
            case 0:
                return INT;
            case 1:
                return VARCHAR;
        }
        return null;
    }
}
