package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.AbstTable;
import cn.liubinbin.kdb.server.table.Column;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class OrderByExePlan extends AbstrExePlan {

    AbstTable table;
    String orderByColumn;
    List<KdbRow> tempSortedData;
    Integer dataNextIdx;
    Integer sortColumnIdx;

    public OrderByExePlan(AbstrExePlan next, AbstTable table, String orderByColumn) {
        this(ExePlanKind.OrderByTable, next, table, orderByColumn);
    }

    public OrderByExePlan(ExePlanKind kind, AbstrExePlan next, AbstTable table, String orderByColumn) {
        super(kind, next);
        this.table = table;
        this.orderByColumn = orderByColumn;
        this.tempSortedData = null;
        this.dataNextIdx = 0;
        this.sortColumnIdx = 0;
        for (int i = 0; i < table.getColumns().size(); i++) {
            Column curColumn = table.getColumns().get(i);
            if (curColumn.getColumnName().equals(orderByColumn)) {
                sortColumnIdx = i;
                break;
            }
        }
    }

    @Override
    public boolean hasMore() {
        return tempSortedData == null || dataNextIdx < tempSortedData.size();
    }

    @Override
    public KdbRow onNext() {
        if (tempSortedData == null){
            tempSortedData = new ArrayList<>();
            ArrayList<KdbRow> tempUnsortData = new ArrayList<>();
            while (next.hasMore()) {
                KdbRow tempRow = next.onNext();
                if (tempRow != null) {
                    tempUnsortData.add(tempRow);
                }

            }
            tempUnsortData.sort((o1, o2) -> {
                    KdbRowValue o1Value = o1.getValues().get(sortColumnIdx);
                    KdbRowValue o2Value = o2.getValues().get(sortColumnIdx);
                    return o1Value.compareTo(o2Value);
                });
                tempSortedData.addAll(tempUnsortData);
        }
        return tempSortedData.get(dataNextIdx++);
    }
}
