package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.table.AbstTable;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class ColumnFilterExePlan extends AbstrExePlan {

    AbstTable table;

    public ColumnFilterExePlan(AbstrExePlan next, AbstTable table) {
        this(ExePlanKind.ColumnFilter, next, table);
    }

    public ColumnFilterExePlan(ExePlanKind kind, AbstrExePlan next, AbstTable table) {
        super(kind, next);
        this.table = table;
    }

    @Override
    public boolean hasMore() {
        return next.hasMore();
    }

    @Override
    public KdbRow onNext() {
        return next.onNext();
    }
}
