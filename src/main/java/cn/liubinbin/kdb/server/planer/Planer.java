package cn.liubinbin.kdb.server.planer;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDescribeSchema;
import org.apache.calcite.sql.SqlDescribeTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;

public class Planer {

    public static Plan plan(SqlNode sqlNode) {
        System.out.println("lbb " + sqlNode.getKind());
        Plan plan = null;
        switch (sqlNode.getKind()) {
            case DESCRIBE_SCHEMA:
                if (sqlNode instanceof SqlDescribeSchema) {
                    SqlDescribeSchema describeSchema = (SqlDescribeSchema) sqlNode;
                    plan = new DescribeDatabasePlan(PlanKind.DESCRIBE_DATABASE, describeSchema.getSchema().getSimple());
                } else {
                    throw new RuntimeException("Expected an DESCRIBE_SCHEMA statement but got: " + sqlNode.getKind());
                }
                break;
            case DESCRIBE_TABLE:
                System.out.println("this is table describe");
                if (sqlNode instanceof SqlDescribeTable) {
                    SqlDescribeTable describeTable = (SqlDescribeTable) sqlNode;
                    System.out.println("Parsed describeTable statement: " + describeTable);
                } else {
                    throw new RuntimeException("Expected an INSERT statement but got: " + sqlNode.getKind());
                }
                break;
            case CREATE_TABLE:
                System.out.println("this is table create");
                if (sqlNode instanceof SqlCreate) {
                    SqlCreateTable create = (SqlCreateTable) sqlNode;
                    System.out.println("Parsed INSERT statement: " + create);
                } else {
                    throw new RuntimeException("Expected an INSERT statement but got: " + sqlNode.getKind());
                }
                break;
            case INSERT:
                System.out.println("this is table insert");
                break;
            case SELECT:
                System.out.println("this is table select");
                break;
            case OTHER:
                System.out.println("do not support");
                break;
        }

        return plan;
    }
}
