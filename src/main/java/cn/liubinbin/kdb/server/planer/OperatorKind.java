package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @date 2024/08/23
 */
public enum OperatorKind {

    EQUAL(0),
    NOT_EQUAL(1),
    GREATER_THAN(2),
    LESS_THAN(3),
    DEFAULT(10);

    private final int kind;

    OperatorKind(int kind) {
        this.kind = kind;
    }

    public int getKind() {
        return kind;
    }

    public static OperatorKind getOperatorKind(int value) {
        switch (value) {
            case 0:
                return EQUAL;
            case 1:
                return NOT_EQUAL;
            case 2:
                return GREATER_THAN;
            case 3:
                return LESS_THAN;
        }
        return null;
    }
}
