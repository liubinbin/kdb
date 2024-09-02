package cn.liubinbin.kdb.sever.table;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.table.TableManage;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class TableManageTest {

    @Test
    public void initShouldRight() throws Exception {
        KdbConfig kdbConfig = new KdbConfig();

        TableManage tableManage = new TableManage(kdbConfig);
        List<String> tableNames = tableManage.ListTableName();
        assertEquals(tableNames.size(), 0);

        tableManage.init();
        tableNames = tableManage.ListTableName();
        assertEquals(tableNames.size(), 2);
        assertTrue(tableNames.contains("stu"));
    }
}
