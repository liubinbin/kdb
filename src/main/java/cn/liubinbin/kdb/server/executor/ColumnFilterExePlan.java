package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.AbstTable;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.utils.Contants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class ColumnFilterExePlan extends AbstrExePlan {

    AbstTable table;
    List<String> columnList;
    List<Integer> displayColumnIdx;
    boolean isCountStar;
    Integer count;

    public ColumnFilterExePlan(AbstrExePlan next, AbstTable table, List<String> columnList) {
        this(ExePlanKind.ColumnFilter, next, table, columnList);
    }

    public ColumnFilterExePlan(ExePlanKind kind, AbstrExePlan next, AbstTable table, List<String> columnList) {
        super(kind, next);
        this.table = table;
        this.columnList = columnList;
        if (columnList.size() == 1 && columnList.get(0).equals(Contants.COUNT_START)) {
            this.isCountStar = true;
            count = Contants.DEFAULT_COUNT_STAR;
        } else {
            this.isCountStar = false;
        }
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
        if (isCountStar) {
            return count == Contants.DEFAULT_COUNT_STAR;
        }
        return nextPlan.hasMore();
    }

    public KdbRow filterRow(KdbRow row) {
        List<KdbRowValue> newValues = new ArrayList<>();
        for (int i = 0; i < displayColumnIdx.size(); i++) {
            newValues.add(row.getValues().get(displayColumnIdx.get(i)));
        }
        return new KdbRow(newValues);
    }

    private KdbRow getNumberKdbRow(Integer number) {
        return new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, number)));
    }

    @Override
    public KdbRow onNext() {
        if (isCountStar) {
            int tempCount = 0;
            while(nextPlan.hasMore()) {
                KdbRow tempRow = nextPlan.onNext();
                if (tempRow != null) {
                    tempCount++;
                }
            }
            this.count = tempCount;
            return getNumberKdbRow(tempCount);
        }
        while(nextPlan.hasMore()) {
            KdbRow tempRow = nextPlan.onNext();
            if (tempRow != null) {
                return filterRow(tempRow);
            }
        }
        return null;
    }
}
