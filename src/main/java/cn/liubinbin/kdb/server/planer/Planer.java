package cn.liubinbin.kdb.server.planer;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;

public class Planer {

    public static Plan plan(SqlNode sqlNode) {

        System.out.println("lbb " + sqlNode.getKind());
        switch (sqlNode.getKind()) {
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

        return null;
    }
}
