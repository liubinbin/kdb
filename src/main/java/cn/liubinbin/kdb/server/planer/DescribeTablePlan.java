package cn.liubinbin.kdb.server.planer;

/**
 * Created by liubinbin on 16/10/31.
 */
public class DescribeTablePlan extends Plan{

    private String tableName;

    public DescribeTablePlan(PlanKind planKind, String tableName) {
        super(planKind);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
