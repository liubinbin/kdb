package cn.liubinbin.kdb.server.planer;


/**
 * @author liubinbin
 */
public abstract class Plan {

    private PlanKind planKind;

    public Plan(PlanKind planKind) {
        this.planKind = planKind;
    }

    public PlanKind getPlanKind() {
        return planKind;
    }
}
