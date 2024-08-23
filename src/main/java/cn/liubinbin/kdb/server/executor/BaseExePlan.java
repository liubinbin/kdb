package cn.liubinbin.kdb.server.executor;

import cn.liubinbin.kdb.server.entity.KdbRow;

public interface BaseExePlan {

    public boolean hasMore();

    public KdbRow onNext();

}
