//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cn.edu.thssdb.index;

import java.util.*;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StoreTest {
    private Table table;
    /*
    private BPlusTree<Integer, Integer> tree;
    private ArrayList<Integer> keys;
    private ArrayList<Integer> values;
    private HashMap<Integer, Integer> map;
    */

    public StoreTest() {
    }

    @Before
    public void setUp() {
        Column column1 = new Column("column1", ColumnType.INT, 1, true, 2);
        Column column2 = new Column("column3", ColumnType.STRING, 0, false, 10);
        Column column3 = new Column("column2", ColumnType.DOUBLE, 0, true, 5);
        Column column4 = new Column("column2", ColumnType.LONG, 0, false, 4);

        Column[] columns = new Column[4];
        columns[0] = column1;
        columns[1] = column2;
        columns[2] = column3;
        columns[3] = column4;
        Table table = new Table("database", "testtable", columns);
    }

    @Test
    public void testInsert() {
        LinkedList values = new LinkedList();
        values.add(new Integer(4));
        values.add(new String("testtest"));
        values.add(new Double(0.4));
        values.add(new Long(2));
        table.insert(values);

    }
/*
    @Test
    public void testRemove() {
        int size = this.keys.size();

        int i;
        for(i = 0; i < size; i += 2) {
            this.tree.remove((Comparable)this.keys.get(i));
        }

        Assert.assertEquals((long)(size / 2), (long)this.tree.size());

        for(i = 1; i < size; i += 2) {
            Assert.assertEquals(this.map.get(this.keys.get(i)), this.tree.get((Comparable)this.keys.get(i)));
        }

    }

    @Test
    public void testIterator() {
        BPlusTreeIterator<Integer, Integer> iterator = this.tree.iterator();

        int c;
        for(c = 0; iterator.hasNext(); ++c) {
            Assert.assertTrue(this.values.contains(iterator.next().getValue()));
        }

        Assert.assertEquals((long)this.values.size(), (long)c);
    }
*/
}
