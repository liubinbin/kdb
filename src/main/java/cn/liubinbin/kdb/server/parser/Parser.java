package cn.liubinbin.kdb.server.parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author liubinbin
 * @info Created by liubinbin on 2024/08/14.
 */
public class Parser {

    public static SqlNode parse(String sql) {
        SqlParser.ConfigBuilder configBuilder = SqlParser.create(sql).configBuilder()
                .setCaseSensitive(false)
                .setLex(Lex.MYSQL)
                .setParserFactory(SqlDdlParserImpl.FACTORY);
        SqlParser parser = SqlParser.create(sql, configBuilder.build());
        try {
            return parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("Error parsing SQL: " + sql, e);
        }
    }

    public static void main(String[] args) {
        List<String> sqls = new ArrayList<>();
        sqls.add("create table a(id int, b int, c varchar(253))"); // id为主键
//        sqls.add("insert into a (id, name) VALUES (1, 'Alice')");
//        sqls.add("select * from a");
//        sqls.add("select * from a where b = 1");
//        sqls.add("select * from a where b = 1 and c = 'haha'");
//        sqls.add("select * from a order by b limit 10");
//        sqls.add("describe database kdb");
//        sqls.add("describe table a");

        for (String sql : sqls) {
            System.out.println("-----");
            SqlNode sqlNode = parse(sql);
            SqlCreateTable create = (SqlCreateTable) sqlNode;
            SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) create.columnList.get(1);
            System.out.println(columnDeclaration.dataType.getTypeNameSpec());
            SqlDataTypeSpec sqlDataTypeSpec = columnDeclaration.dataType;
            SqlBasicTypeNameSpec sqlBasicTypeNameSpec = (SqlBasicTypeNameSpec)sqlDataTypeSpec.getTypeNameSpec();
            System.out.println(sqlBasicTypeNameSpec.getPrecision());
            System.out.println(sqlBasicTypeNameSpec.getTypeName());
        }
    }
}
