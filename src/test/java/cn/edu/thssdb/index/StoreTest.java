package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;

import static org.junit.Assert.*;

public class StoreTest {
    private Table table;

    @Before
    public void setUp() throws IOException {
        Column column1 = new Column("column1", ColumnType.INT, 1, true, 2);
        Column column2 = new Column("column2", ColumnType.STRING, 0, false, 30);
        Column column3 = new Column("column3", ColumnType.DOUBLE, 0, true, 5);
        Column column4 = new Column("column4", ColumnType.LONG, 0, false, 4);


        Column[] columns = new Column[4];
        columns[0] = column1;
        columns[1] = column2;
        columns[2] = column3;
        columns[3] = column4;
        table = new Table("database", "testtable", columns);


    }

    @Test
    public void testInsert() {
        try {
            LinkedList data1 = new LinkedList();
            data1.add(new Integer(1));
            data1.add(new String("Zhang"));
            data1.add(new Double(1.1));
            data1.add(new Long(111));
            table.insert(data1);

            LinkedList data2 = new LinkedList();
            data2.add(new Integer(2));
            data2.add(new String("Shu Ju Ku"));
            data2.add(new Double(2.2));
            data2.add(new Long(2222));
            table.insert(data2);

            LinkedList data3 = new LinkedList();
            data3.add(new Integer(3));
            data3.add(null);
            data3.add(new Double(3.3));
            data3.add(new Long(3333));
            table.insert(data3);

            LinkedList data4 = new LinkedList();
            data4.add(new Integer(4));
            data4.add(new String("Yi Dong Kai Fa"));
            data4.add(new Double(4.4));
            data4.add(null);
            table.insert(data4);
        } catch (Exception e) {
            //不应该抛出异常
            fail();
        }
        try {
            LinkedList data5 = new LinkedList();
            data5.add(new Integer(4));
            data5.add(new String("Ruan Jian Fen Xi"));
            data5.add(new Double(5.5));
            data5.add(new Long(5555));
            table.insert(data5);

            //应该在这里抛出主键重复的异常
            fail();
        } catch (Exception e) {
            System.out.println(e.toString());
        }
    }

    @Test
    public void testDelete() {

    }

    @Test
    public void testUpdate() {

    }

    @Test
    public void recover() {

    }
}
