package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.planer.BoolExpression;
import cn.liubinbin.kdb.server.table.AbstTable;

import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class WhereExePlan extends AbstrExePlan {

    AbstTable table;
    private List<BoolExpression> boolExpressions;

    public WhereExePlan(ExePlanKind kind, AbstrExePlan nextPlan, AbstTable table, List<BoolExpression> boolExpressions) {
        super(kind, nextPlan);
        this.table = table;
        this.boolExpressions = boolExpressions;
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
