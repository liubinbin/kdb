package cn.liubinbin.kdb.server.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class Parser {

    // Statement parse(String sqlString){
    //     return null;
    // }

    public static SqlNode parse(String sql) {
        // SqlParser.ConfigBuilder configBuilder = SqlParser.create(sql).configBuilder()
        //         .setCaseSensitive(false)
        //         .setUnquotedCasing(SqlParser.Config.UnquotedCasing.UNCHANGED);
        SqlParser parser = SqlParser.create(sql);
        try {
            return parser.parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException("Error parsing SQL: " + sql, e);
        }
    }
}
