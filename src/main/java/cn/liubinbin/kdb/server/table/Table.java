package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.Row;

import java.util.List;

public interface Table {

    public List<Row> limit(Integer limit);
}
