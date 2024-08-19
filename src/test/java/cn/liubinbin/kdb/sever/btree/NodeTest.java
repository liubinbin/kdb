package cn.liubinbin.kdb.sever.btree;

import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class NodeTest {

    @Test
    public void shouldAnswerWithTrue() {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
        KdbRow rowSix = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 6)));
        Node node = new Node(true, true, 1);
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowThree);
        node.add(rowFour);
//        node.add(rowFive);
//        node.add(rowSix);
        node.print();
        node.removeBigThan(rowThree);
        node.print();
        assertTrue(true);
    }

}
