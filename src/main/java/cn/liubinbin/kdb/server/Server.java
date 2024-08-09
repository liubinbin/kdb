package cn.liubinbin.kdb.server;

import cn.liubinbin.kdb.server.parser.Parser;
import org.apache.calcite.sql.SqlNode;

public class Server {

    public void start(){

    }

    public void localMock(){
        System.out.println("local mock");
        SqlNode sqlNode = Parser.parse("select a,b from c");
        System.out.println(sqlNode);
    }

    public static void main(String[] args) {
        // new Server().start();
        new Server().localMock();
    }
    
}
