package cn.liubinbin.kdb.server.table;

import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.planer.BoolExpression;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liubinbin
 */
public class FakeTable extends AbstTable {

    public FakeTable(String tableName, List<Column> columns) {
        super(tableName, columns, TableType.Fake);
    }

    @Override
    public List<KdbRow> limit(Integer limit) {
        List<KdbRow> list = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            KdbRow temp = new KdbRow();
            temp.appendRowValue(new KdbRowValue(ColumnType.VARCHAR, "hello world " + i));
            list.add(temp);
        }
        return list;
    }

    @Override
    public void insert(KdbRow rowToInsert) {
        System.out.println("insert row into table");
    }

    @Override
    public boolean isValid(KdbRow rowToCheck) {
        return true;
    }

    @Override
    public void delete(List<BoolExpression> expressions) {

    }

    @Override
    public void select() {

    }
}
