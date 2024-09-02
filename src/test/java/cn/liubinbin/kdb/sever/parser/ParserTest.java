package cn.liubinbin.kdb.sever.parser;


import cn.liubinbin.kdb.server.parser.Parser;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;

import static org.junit.Assert.assertSame;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class ParserTest {

    @Test
    public void parseDescDbShouldRight() {
        String sql = "describe database kdb";
        SqlNode sqlNode = Parser.parse(sql);

        assertSame(sqlNode.getKind(), SqlKind.DESCRIBE_SCHEMA);
    }

    @Test
    public void parseDescTableShouldRight() {
        String sql = "describe table stu";
        SqlNode sqlNode = Parser.parse(sql);

        assertSame(sqlNode.getKind(), SqlKind.DESCRIBE_TABLE);
    }

    @Test
    public void parseInsertTableShouldRight() {
        String sql = "insert into stu(id, age, name) VALUES (1, 29, 'Alice')";
        SqlNode sqlNode = Parser.parse(sql);

        assertSame(sqlNode.getKind(), SqlKind.INSERT);
    }

    @Test
    public void parseCreateTableShouldRight() {
        String sql = "create table stu(id int, age int, name varchar(256))";
        SqlNode sqlNode = Parser.parse(sql);

        assertSame(sqlNode.getKind(), SqlKind.CREATE_TABLE);
    }

    @Test
    public void parseSelectTableShouldRight() {
        String sql = "select id,age,name from stu";
        SqlNode sqlNode = Parser.parse(sql);

        assertSame(sqlNode.getKind(), SqlKind.SELECT);
    }

    @Test
    public void parseDeleteTableShouldRight() {
        String sql = "delete from stu where id = 1 and name = 'haha'";
        SqlNode sqlNode = Parser.parse(sql);

        assertSame(sqlNode.getKind(), SqlKind.DELETE);
    }
}
