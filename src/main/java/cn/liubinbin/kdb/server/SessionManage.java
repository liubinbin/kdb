package cn.liubinbin.kdb.server;

public class SessionManage {

    public void execute(Session session) {
        Parser parser = new Parser();
        Statement statement = parser.parse(session.getStatement());
        Engine engine = new Engine();
        engine.execute(statement);
    }

    public Statement prepare_statement(String sqlString) {
        Parser parser = new Parser();
        Statement statement = parser.parse(sqlString);
        return statement;
    }


    public void execute(Session... sessions) {
        
    }

}
