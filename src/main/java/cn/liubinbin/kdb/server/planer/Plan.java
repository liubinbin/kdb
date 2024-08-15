package cn.liubinbin.kdb.server.planer;

public abstract class Plan {

    private PlanKind planKind;

    public Plan(PlanKind planKind) {
        this.planKind = planKind;
    }

    public PlanKind getPlanKind() {
        return planKind;
    }
}
