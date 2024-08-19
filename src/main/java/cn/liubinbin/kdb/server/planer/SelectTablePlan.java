package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/17.
 */
public class SelectTablePlan extends Plan {

    private String tableName;
    private String order;
    private String limit;
    private String filters;
    private String groupBy;

    public SelectTablePlan(String tableName) {
        super(PlanKind.INSERT_TABLE);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
