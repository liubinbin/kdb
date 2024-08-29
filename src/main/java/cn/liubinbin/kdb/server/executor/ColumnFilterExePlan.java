package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.AbstTable;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class ColumnFilterExePlan extends AbstrExePlan {

    AbstTable table;
    List<String> columnList;
    List<Integer> displayColumnIdx;

    public ColumnFilterExePlan(AbstrExePlan next, AbstTable table, List<String> columnList) {
        this(ExePlanKind.ColumnFilter, next, table, columnList);
    }

    public ColumnFilterExePlan(ExePlanKind kind, AbstrExePlan next, AbstTable table, List<String> columnList) {
        super(kind, next);
        this.table = table;
        this.columnList = columnList;
        this.displayColumnIdx = new ArrayList<>();
        for (int i = 0; i < table.getColumns().size(); i++) {
            if (columnList.contains(table.getColumns().get(i).getColumnName())) {
                if (!displayColumnIdx.contains(i)) {
                    displayColumnIdx.add(i);
                }
            }
        }
    }

    @Override
    public boolean hasMore() {
        return next.hasMore();
    }

    public KdbRow filterRow(KdbRow row) {
        List<KdbRowValue> newValues = new ArrayList<>();
        for (int i = 0; i < displayColumnIdx.size(); i++) {
            newValues.add(row.getValues().get(displayColumnIdx.get(i)));
        }
        row.setValues(newValues);
        return row;
    }

    @Override
    public KdbRow onNext() {
        while(next.hasMore()) {
            KdbRow tempRow = next.onNext();
            if (tempRow != null) {
                return filterRow(tempRow);
            }
        }
        return null;
    }
}
