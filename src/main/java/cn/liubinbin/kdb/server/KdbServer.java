package cn.liubinbin.kdb.server;

import cn.liubinbin.kdb.conf.Config;
import cn.liubinbin.kdb.server.interf.KdbGrpcServer;
import cn.liubinbin.kdb.server.parser.Parser;
import cn.liubinbin.kdb.server.table.TableManage;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KdbServer {

    public void doStart() throws IOException, InterruptedException, ConfigurationException {
        // 配置文件
        Config kdbConfig = new Config();

        // 表信息初始化
        TableManage tableManage = new TableManage(kdbConfig);
        tableManage.init();

        // gRPC server 初始化和启动
        final KdbGrpcServer server = new KdbGrpcServer(kdbConfig, tableManage);
        server.start();
        server.blockUntilShutdown();
    }

    public void localMock() {
        List<String> sqls = new ArrayList<>();
        sqls.add("create table a(id int, b int, c varchar(256))"); // id为主键
        sqls.add("insert into a (id, name) VALUES (1, 'Alice')");
        sqls.add("select * from a");
        sqls.add("select * from a where b = 1");
        sqls.add("select * from a where b = 1 and c = 'haha'");
        sqls.add("select * from a order by b limit 10");
        sqls.add("select a,b from c");
        for (String sql : sqls) {
            System.out.println("-----");
            SqlNode sqlNode = Parser.parse(sql);
            System.out.println(sqlNode.getKind());
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException {
//        new KdbServer().doStart();
        new KdbServer().localMock();
    }

}
