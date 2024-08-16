package cn.liubinbin.kdb.server.table;

import org.apache.commons.lang3.StringUtils;

/**
 * @author liubinbin
 * @info Created by liubinbin on 16/10/30.
 */
public enum ColumnType {

    INTEGER(0, "INTEGER"),
    VARCHAR(1, "VARCHAR");

    private final int columnTypeInt;
    private final String ColumnTypeStr;

    ColumnType(int columnTypeInt) {
        ColumnType columnType = getColumnType(columnTypeInt);
        if (columnType == null) {
            throw new IllegalArgumentException("Illegal ColumnType value: " + columnTypeInt);
        }
        this.columnTypeInt = columnTypeInt;
        this.ColumnTypeStr = columnType.ColumnTypeStr;
    }

    ColumnType(int columnTypeInt, String ColumnTypeStr) {
        this.columnTypeInt = columnTypeInt;
        this.ColumnTypeStr = ColumnTypeStr;
    }

    public static ColumnType getColumnType(int value) {
        switch (value) {
            case 0:
                return INTEGER;
            case 1:
                return VARCHAR;
        }
        return null;
    }

    public static ColumnType getColumnType(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        if (value.equalsIgnoreCase(INTEGER.ColumnTypeStr)) {
            return INTEGER;
        }
        if (value.equalsIgnoreCase(VARCHAR.ColumnTypeStr)) {
            return VARCHAR;
        }
        return null;
    }

}
