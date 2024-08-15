package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.KdbRow;

import java.util.List;

public interface Table {

    public List<KdbRow> limit(Integer limit);
}
