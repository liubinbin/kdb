package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/14.
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
