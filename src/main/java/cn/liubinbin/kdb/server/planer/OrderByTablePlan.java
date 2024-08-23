package cn.liubinbin.kdb.server.planer;

import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/17.
 */
public class OrderByTablePlan extends Plan {

    private final String tableName;
    private String columnOrderBy;
    private Integer limit;
    private List<BoolExpression> whereBoolExpreList;
    private String columnGroupBy;

    public OrderByTablePlan(String tableName) {
        super(PlanKind.SELECT_TABLE);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
