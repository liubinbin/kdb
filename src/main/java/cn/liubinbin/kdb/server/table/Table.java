package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.KdbRow;

import java.util.List;


/**
 * @author liubinbin
 */
public interface Table {

    public List<KdbRow> limit(Integer limit);
}
