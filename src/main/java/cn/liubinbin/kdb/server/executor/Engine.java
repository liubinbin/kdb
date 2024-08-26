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
        // scan

        // whereFilter

        // order by

        // column filter

        // limit

        // return;
    }
}
