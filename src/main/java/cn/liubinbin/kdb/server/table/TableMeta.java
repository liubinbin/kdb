package cn.liubinbin.kdb.server.table;

import java.io.File;
import java.util.List;

/**
 * 暂只支持 integer 和 varchar
 */
public class TableMeta {

    String tableName;
    List<Column> columnList;

    public void writeTo(File file, int offset) {

    }

}
