package cn.liubinbin.kdb.server.executor;

public abstract class AbstrExePlan {

    public int kind;

    public AbstrExePlan(int kind) {
        this.kind = kind;
    }

}
