package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/16.
 */
public class InsertTablePlan extends Plan {

    private String tableName;

    public InsertTablePlan(String tableName) {
        super(PlanKind.INSERT_TABLE);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
