package cn.liubinbin.kdb.server.planer;

/**
 * Created by liubinbin on 16/10/31.
 */
public class DescribeDatabasePlan extends Plan{

    private String dbName;

    public DescribeDatabasePlan(PlanKind planKind, String dbName) {
        super(planKind);
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

}
