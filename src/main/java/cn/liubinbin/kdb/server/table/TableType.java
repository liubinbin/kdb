package cn.liubinbin.kdb.server.table;

/**
 * @author liubinbin
 * @date 2024/08/16
 */
public enum TableType {

    Btree(0, "btree"),
    Fake(1, "fake");

    private final int type;
    private final String name;

    TableType(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public static TableType getTableType(int value) {
        switch (value)
        {
            case 0:
                return Btree;
            case 1:
                return Fake;
            default:
                return null;
        }
   }

    public static TableType getTableType(String name) {
        for (TableType tableType : TableType.values()) {
            if (tableType.name.equals(name)) {
                return tableType;
            }
        }
        return null;
    }
}
