package cn.liubinbin.kdb.server.planer;

import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/17.
 */
public class DeleteTablePlan extends Plan {

    private final String tableName;
    private List<BoolExpression> whereBoolExpreList;

    public DeleteTablePlan(String tableName) {
        super(PlanKind.DELETE_TABLE);
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

}
