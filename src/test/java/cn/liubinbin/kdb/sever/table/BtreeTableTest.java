package cn.liubinbin.kdb.sever.table;


import cn.liubinbin.kdb.conf.KdbConfig;
import cn.liubinbin.kdb.server.btree.Cursor;
import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.store.TableStore;
import cn.liubinbin.kdb.server.table.BtreeTable;
import cn.liubinbin.kdb.server.table.Column;
import cn.liubinbin.kdb.server.table.ColumnType;
import cn.liubinbin.kdb.server.table.TableType;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author liubinbin
 * @date 2024/09/01
 */
public class BtreeTableTest {

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
    public void initAndLimitShouldRight() throws ConfigurationException, IOException {
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

        btreeTable = getFakeTable();
        btreeTable.init();
        List<KdbRow> rows = btreeTable.limit(1);

        assertEquals(1, rows.size());
    }

    @Test
    public void insertShouldRight(){
        BtreeTable btreeTable = getFakeTable();
        KdbRow row = new KdbRow();
        KdbRowValue id = new KdbRowValue(ColumnType.INTEGER, 4);
        KdbRowValue name = new KdbRowValue(ColumnType.VARCHAR, "bin4");
        row.appendRowValue(id);
        row.appendRowValue(name);

        btreeTable.insert(row);

        Cursor cursor = btreeTable.getCursor();
        assertTrue(cursor.hasMore());
        KdbRow nextRow =cursor.next();
        assertEquals(4, nextRow.getRowKey().longValue());
        assertEquals("bin4", nextRow.getValues().get(1).getStringValue());

    }
}
