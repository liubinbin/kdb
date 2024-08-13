package cn.liubinbin.kdb.server.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.util.ArrayList;
import java.util.List;

public class Parser {

    // Statement parse(String sqlString){
    //     return null;
    // }

    public static SqlNode parse(String sql) {
         SqlParser.ConfigBuilder configBuilder = SqlParser.create(sql).configBuilder()
                 .setCaseSensitive(false)
                 .setLex(Lex.MYSQL)
                 .setParserFactory(SqlDdlParserImpl.FACTORY);;
//                 .setUnquotedCasing(SqlParser.Config.UnquotedCasing.UNCHANGED);
        SqlParser parser = SqlParser.create(sql, configBuilder.build());
        try {
            return parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("Error parsing SQL: " + sql, e);
        }
    }

    public static void main(String[] args) {
        List<String> sqls = new ArrayList<>();
        sqls.add("create table a(id int, b int, c varchar(256))"); // id为主键
//        sqls.add("insert into a (1, 'helloworld')");
        sqls.add("select * from a");
        sqls.add("select * from a where b = 1");
        sqls.add("select * from a where b = 1 and c = 'haha'");
        sqls.add("select * from a order by b limit 10");
        for (String sql : sqls) {
            System.out.println("-----");
            SqlNode sqlNode = parse(sql);
            System.out.println(sqlNode.getKind());
        }

    }
}
