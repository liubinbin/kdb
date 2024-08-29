package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;

public abstract class AbstrExePlan implements BaseExePlan {

    public ExePlanKind kind;
    public AbstrExePlan next;

    public AbstrExePlan(ExePlanKind kind) {
        this.kind = kind;
        this.next = null;
    }

    public AbstrExePlan(ExePlanKind kind, AbstrExePlan next) {
        this.kind = kind;
        this.next = next;
    }

    public boolean hasNext() {
        return next != null;
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
