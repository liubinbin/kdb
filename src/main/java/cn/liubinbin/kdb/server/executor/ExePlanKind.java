package cn.liubinbin.kdb.server.executor;


/**
 * @author liubinbin
 * @date 2024/08/27
 * 物理执行计划类型
 */
public enum ExePlanKind {

    ScanTable(0),
    WhereFilter(1),
    OrderByTable(2),
    ColumnFilter(3),
    Limit(4),
    DEFAULT(10);

    private final int kind;

    ExePlanKind(int kind) {
        this.kind = kind;
    }

    public int getKind() {
        return kind;
    }

    public static ExePlanKind getExePlanKind(int value) {
        switch (value) {
            case 0:
                return ScanTable;
            case 1:
                return WhereFilter;
            case 2:
                return OrderByTable;
            case 3:
                return ColumnFilter;
            case 4:
        }
    }
}
