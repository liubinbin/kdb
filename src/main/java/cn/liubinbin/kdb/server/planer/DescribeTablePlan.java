package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/14.
 */
public class DescribeTablePlan extends Plan{

    private String tableName;

    public DescribeTablePlan(String tableName) {
        super(PlanKind.DESCRIBE_TABLE);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
