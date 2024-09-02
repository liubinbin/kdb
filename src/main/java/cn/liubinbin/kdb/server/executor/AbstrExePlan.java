package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;

public abstract class AbstrExePlan implements BaseExePlan {

    public ExePlanKind kind;
    public AbstrExePlan nextPlan;

    public AbstrExePlan(ExePlanKind kind) {
        this.kind = kind;
        this.nextPlan = null;
    }

    public AbstrExePlan(ExePlanKind kind, AbstrExePlan nextPlan) {
        this.kind = kind;
        this.nextPlan = nextPlan;
    }

    public boolean hasNextPlan() {
        return nextPlan != null;
    }

    @Override
    public boolean hasMore() {
        return false;
    }

    @Override
    public KdbRow onNext() {
        return null;
    }

    @Override
    public String toString() {
        return "This is a " + kind.toString() + " plan";
    }
}
