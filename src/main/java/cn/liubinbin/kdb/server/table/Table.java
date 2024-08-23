package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.planer.BoolExpression;

import java.util.List;


/**
 * @author liubinbin
 */
public interface Table {

    public List<KdbRow> limit(Integer limit);

    public void insert(KdbRow rowToInsert);

    public void delete(List<BoolExpression> expressions);

    public void select();

}
