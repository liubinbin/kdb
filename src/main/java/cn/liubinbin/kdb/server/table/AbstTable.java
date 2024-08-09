package cn.liubinbin.kdb.server.table;

public abstract class AbstTable implements Table{

    String tableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
