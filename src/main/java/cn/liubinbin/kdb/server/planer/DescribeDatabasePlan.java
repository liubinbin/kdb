package cn.liubinbin.kdb.server.planer;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/14.
 */
public class DescribeDatabasePlan extends Plan{

    private String dbName;

    public DescribeDatabasePlan(String dbName) {
        super(PlanKind.DESCRIBE_DATABASE);
        this.dbName = dbName;
    }

    public String getDbName() {
        return dbName;
    }

}
