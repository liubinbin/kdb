package cn.liubinbin.kdb.sever.store;

import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.store.TableStore;
import cn.liubinbin.kdb.server.table.BtreeTable;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.server.table.TableType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author liubinbin
 * @date 2024/09/02
 */
public class TableStoreTest {

    private BtreeTable getFakeTable(){
        List<Column> tableColumn = new ArrayList<>();
        tableColumn.add(new Column(0, "id", ColumnType.INTEGER, 0));
        tableColumn.add(new Column(1, "name", ColumnType.VARCHAR, 128));

        return new BtreeTable("tempta", tableColumn);
    }

    private List<KdbRow> getIdKdbRowList(int count){
        List<KdbRow> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            KdbRow row = new KdbRow();
            KdbRowValue id = new KdbRowValue(ColumnType.INTEGER, i);
            KdbRowValue name = new KdbRowValue(ColumnType.VARCHAR, "bin"+i);
            row.appendRowValue(id);
            row.appendRowValue(name);
            data.add(row);
        }
        return data;
    }

    @Test
    public void testDataFileWriteRead() throws Exception {
        BtreeTable btreeTable = getFakeTable();
        KdbConfig kdbConfig = new KdbConfig();
        TableStore tableStore = new TableStore("tempta", kdbConfig, btreeTable.getColumns(), TableType.Btree);

        Node node = new Node(true, true, 0, kdbConfig.getBtreeOrder());

        List<KdbRow> kdbRowList = getIdKdbRowList(3);
        for (KdbRow curRow: kdbRowList) {
            node.add(curRow);
        }

        tableStore.registerNode(node);
        assertEquals(1, tableStore.getNodeMap().size());

        tableStore.close();

        TableStore newTableStore = new TableStore("tempta", kdbConfig, btreeTable.getColumns(), TableType.Btree);
        newTableStore.readDataFile();

        assertEquals(1, newTableStore.getNodeMap().size());
    }
}
