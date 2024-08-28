package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/17.
 */
public class OrderByTablePlan extends Plan {

    private final String tableName;
    private String columnOrderBy;
    private Integer limit;

    public OrderByTablePlan(String tableName) {
        super(PlanKind.SELECT_TABLE);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
