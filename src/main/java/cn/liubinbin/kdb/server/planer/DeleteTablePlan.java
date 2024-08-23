package cn.liubinbin.kdb.server.planer;

import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/23.
 */
public class DeleteTablePlan extends Plan {

    private final String tableName;
    private List<BoolExpression> whereBoolExpreList;

    public DeleteTablePlan(String tableName, List<BoolExpression> whereBoolExpreList) {
        super(PlanKind.DELETE_TABLE);
        this.tableName = tableName;
        this.whereBoolExpreList = whereBoolExpreList;
    }

    public String getTableName() {
        return tableName;
    }

    public List<BoolExpression> getWhereBoolExpreList() {
        return whereBoolExpreList;
    }

    @Override
    public String toString() {
        return "DeleteTablePlan{" +
                "tableName='" + tableName + '\'' +
                ", whereBoolExpreList=" + whereBoolExpreList +
                '}';
    }
}
