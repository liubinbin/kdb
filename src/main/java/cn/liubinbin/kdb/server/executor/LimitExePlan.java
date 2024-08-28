package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.table.AbstTable;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class LimitExePlan extends AbstrExePlan {

    AbstTable table;
    Integer limit;
    Integer currentCount;

    public LimitExePlan(ExePlanKind kind, AbstrExePlan next, Integer limit) {
        super(kind, next);
        this.limit = limit;
        this.currentCount = 0;
    }

    @Override
    public boolean hasMore() {
        return currentCount <= limit && next.hasMore();
    }

    @Override
    public KdbRow onNext() {
        if (currentCount < limit) {
            currentCount++;
            return next.onNext();
        } else {
            return null;
        }
    }
}
