package cn.liubinbin.kdb.server.planer;

/**
 * Created by liubinbin on 16/11/15.
 */
public enum PlanKind {

    DESCRIBE_DATABASE(0),
    DESCRIBE_TABLE(1),
    CREATE_TABLE(2),
    INSERT_TABLE(3),
    SELECT_TABLE(4),
    DELETE_TABLE(5),
    DEFAULT(10);

    private final int kind;

    PlanKind(int kind) {
        this.kind = kind;
    }

    public int getKind() {
        return kind;
    }

    public static PlanKind getPlanKind(int value) {
        for (PlanKind planKind : PlanKind.values()) {
            if (planKind.getKind() == value) {
                return planKind;
            }
        }
        return null;
    }
}
