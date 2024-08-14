package cn.liubinbin.kdb.server;

import cn.liubinbin.kdb.conf.Config;
import cn.liubinbin.kdb.server.interf.KdbGrpcServer;
import cn.liubinbin.kdb.server.parser.Parser;
import cn.liubinbin.kdb.server.table.TableManage;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;

public class KdbServer {

    public void doStart() throws IOException, InterruptedException, ConfigurationException {
        // 配置文件
        Config kdbConfig = new Config();

        // 初始化
        TableManage tableManage = new TableManage();
        tableManage.init();

        final KdbGrpcServer server = new KdbGrpcServer(tableManage);
        server.start();
        server.blockUntilShutdown();
    }

    public void localMock() {
        System.out.println("local mock");
        SqlNode sqlNode = Parser.parse("select a,b from c");
        System.out.println(sqlNode);
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException {
        new KdbServer().doStart();
//        new KdbServer().localMock();
    }

}
