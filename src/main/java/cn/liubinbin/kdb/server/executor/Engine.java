package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.planer.SelectTablePlan;
import cn.liubinbin.kdb.server.table.AbstTable;

/**
 * @author liubinbin
 */
public class Engine {

    public static final Engine INSTANCE = null;

    protected Engine() {

    }

    private static class SingletonHolder {
        private static final Engine INSTANCE = new Engine();
    }

    public static Engine getInstance() {
        return SingletonHolder.INSTANCE;
    }

    public AbstrExePlan generatePhysicalPlan(SelectTablePlan plan, AbstTable table) {
        AbstrExePlan curFirstNode = null;
        // scan
        curFirstNode = new ScanExePlan(null, table);

        // whereFilter
        AbstrExePlan whereFilter = null;
        if (plan.getWhereBoolExpreList() != null && !plan.getWhereBoolExpreList().isEmpty()) {
            whereFilter = new WhereExePlan(curFirstNode, table, plan.getWhereBoolExpreList(),
                    plan.isWhereAnd());
            curFirstNode = whereFilter;
        }

        // order by
        OrderByExePlan orderByExePlan = null;
        if (plan.isOrderBy()) {
            orderByExePlan = new OrderByExePlan(curFirstNode, table, plan.getColumnOrderBy());
            curFirstNode = orderByExePlan;
        }

        // column filter
        ColumnFilterExePlan columnFilterExePlan = null;
        if (plan.getColumnList() != null && !plan.getColumnList().isEmpty()) {
            columnFilterExePlan = new ColumnFilterExePlan(curFirstNode, table, plan.getColumnList());
            curFirstNode = columnFilterExePlan;
        }

        // limit
        LimitExePlan limitExePlan = null;
        if (plan.getLimit() != null && plan.getLimit() > 0) {
            limitExePlan = new LimitExePlan(curFirstNode, plan.getLimit());
            curFirstNode = limitExePlan;
        }

        System.out.println("generatePhysicalPlan FirstNode: " + curFirstNode);
        return curFirstNode;
    }
}
