package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;

public class ScanExePlan extends AbstrExePlan implements BaseExePlan{

    public ScanExePlan(int kind) {
        super(kind);
    }

    @Override
    public boolean hasMore() {
        return false;
    }

    @Override
    public KdbRow onNext() {
        return null;
    }
}
