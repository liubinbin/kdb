package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liubinbin
 * @date 2024/08/27
 */
public class FakeScanExePlan extends AbstrExePlan {

    private final List<KdbRow> data;
    private Integer curIdx;

    public FakeScanExePlan(AbstrExePlan nextPlan, List<KdbRow> data) {
        this(ExePlanKind.ScanTable, nextPlan, data);
    }

    public FakeScanExePlan(ExePlanKind kind, AbstrExePlan nextPlan, List<KdbRow> data) {
        super(kind, nextPlan);
        if (data == null) {
            this.data = new ArrayList<>();
        } else {
            this.data = data;
        }
        this.curIdx = 0;
    }

    @Override
    public boolean hasMore() {
        return curIdx < data.size();
    }

    @Override
    public KdbRow onNext() {
        if (curIdx < data.size()) {
            return data.get(curIdx++);
        } else {
            return null;
        }
    }
}
