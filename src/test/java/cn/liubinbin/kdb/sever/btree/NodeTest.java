package cn.liubinbin.kdb.sever.btree;

import cn.liubinbin.kdb.server.btree.Node;
import cn.liubinbin.kdb.server.entity.KdbRow;
import cn.liubinbin.kdb.server.entity.KdbRowValue;
import cn.liubinbin.kdb.server.table.ColumnType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

public class NodeTest {

    @Test
    public void addShouldRight() {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
        KdbRow rowSix = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 6)));
        Node node = new Node(true, true, 1, 6);
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowThree);
        node.add(rowFour);
        node.add(rowFive);
        assertEquals( 0,node.getCurrentRowCount() - 5);
        node.add(rowSix);
        assertEquals( 0,node.getCurrentRowCount() - 5);
        node.print();

        node.add(rowOne);
    }

    @Test
    public void addCannotDuplicate() {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        Node node = new Node(true, true, 1, 5);
        boolean canDuplicate = true;
        node.add(rowOne);
        try {
            node.add(rowOne);
        } catch (Exception e) {
            canDuplicate = false;
        }
        assertFalse(canDuplicate);
    }

    @Test
    public void removeBigThanShouldRight() {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
        Node node = new Node(true, true, 1, 5);
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowFour);
        node.add(rowFive);
        node.print();

        node.removeBigThan(rowThree);
        assertEquals( 0,node.getCurrentRowCount() - 2);
    }

    @Test
    public void removeSmallThanShouldRight() {
        KdbRow rowOne = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 1)));
        KdbRow rowTwo = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 2)));
        KdbRow rowThree = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 3)));
        KdbRow rowFour = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 4)));
        KdbRow rowFive = new KdbRow(Collections.singletonList(new KdbRowValue(ColumnType.INTEGER, 5)));
        Node node = new Node(true, true, 1, 5);
        node.add(rowOne);
        node.add(rowTwo);
        node.add(rowFour);
        node.add(rowFive);
        node.print();

        node.removeSmallThan(rowThree);
        assertEquals( 0,node.getCurrentRowCount() - 2);
    }

}
