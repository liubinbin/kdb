package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.btree.Cursor;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.table.AbstTable;
import cn.liubinbin.kdb.server.table.BtreeTable;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class OrderByExePlan extends AbstrExePlan {

    AbstTable table;
    Cursor cursor;

    public OrderByExePlan(ExePlanKind kind, AbstrExePlan next, AbstTable table) {
        super(kind, next);
        this.table = table;
        if (table instanceof BtreeTable) {
            cursor = ((BtreeTable) table).getCursor();
        }
    }

    @Override
    public boolean hasMore() {
        return cursor.hasMore();
    }

    @Override
    public KdbRow onNext() {
        return cursor.next();
    }
}