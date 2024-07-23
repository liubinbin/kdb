package cn.liubinbin.kdb.server;

public class Session {

    private String sqlString;
    private Statement statement;

    Session(String sqlString, Statement statement) {
        this.sqlString = sqlString;
        this.statement = statement;
    }
    
    public String getSqlString() {
        return sqlString;
    }
    public Statement getStatement() {
        return statement;
    }


}
