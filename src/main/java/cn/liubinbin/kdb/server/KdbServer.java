package cn.liubinbin.kdb.server;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.interf.KdbGrpcServer;
import cn.liubinbin.kdb.server.table.TableManage;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;

/**
 * @author liubinbin
 * @date 2024/8/14
 */
public class KdbServer {

    public void doStart() throws IOException, InterruptedException, ConfigurationException {
        // 配置文件
        KdbConfig kdbConfig = new KdbConfig();

        // 表信息初始化
        TableManage tableManage = new TableManage(kdbConfig);
        tableManage.init();

        // 存储初始化
//        StoreManage storeManage = new StoreManage(kdbConfig, tableManage);
//        storeManage.init();

        // gRPC server 初始化和启动
        final KdbGrpcServer server = new KdbGrpcServer(kdbConfig, tableManage);
        server.start();
        server.blockUntilShutdown();
    }

    public static void main(String[] args) throws IOException, InterruptedException, ConfigurationException {
        new KdbServer().doStart();
    }

}
