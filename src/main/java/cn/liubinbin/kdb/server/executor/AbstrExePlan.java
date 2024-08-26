package cn.liubinbin.kdb.server.executor;

public abstract class AbstrExePlan implements BaseExePlan {

    public int kind;
    public AbstrExePlan next;

    public AbstrExePlan(int kind) {
        this.kind = kind;
    }

}
