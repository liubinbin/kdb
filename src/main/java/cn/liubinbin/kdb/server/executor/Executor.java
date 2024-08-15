package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.grpc.Header;
import cn.liubinbin.kdb.grpc.KdbSqlResponse;
import cn.liubinbin.kdb.server.entity.Row;
import cn.liubinbin.kdb.server.planer.Plan;
import cn.liubinbin.kdb.server.table.TableManage;

import java.util.List;

/**
 * Created by liubinbin on 16/10/31.
 */
public class Executor {

    private TableManage tableManage = null;

    public Executor(TableManage tableManage) {
        this.tableManage = tableManage;
    }

    public KdbSqlResponse execute(Plan plan) {
        List<Row> rows = tableManage.getTable("test").limit(1);
        Header header = Header.newBuilder().addHeader("bin header").build();
        cn.liubinbin.kdb.grpc.Row row = cn.liubinbin.kdb.grpc.Row.newBuilder().addValue(rows.get(0).getValues().get(0)).build();
        KdbSqlResponse reply = KdbSqlResponse.newBuilder().setHeader(header).addRow(row).build();
        return reply;
    }
}
