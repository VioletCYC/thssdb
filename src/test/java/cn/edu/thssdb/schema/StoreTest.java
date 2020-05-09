
package cn.edu.thssdb.schema;

import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.LinkedList;

import static org.junit.Assert.*;

//按字母顺序执行测试用例
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StoreTest {
    private Table table;
    private String tableName = "testTable";

    @Before
    public void setUp()
            throws IOException, ClassNotFoundException {
        Column column1 = new Column("column1", ColumnType.INT, 1, true, 2);
        Column column2 = new Column("column2", ColumnType.STRING, 0, false, 30);
        Column column3 = new Column("column3", ColumnType.DOUBLE, 0, true, 5);
        Column column4 = new Column("column4", ColumnType.LONG, 0, false, 4);


        Column[] columns = new Column[4];
        columns[0] = column1;
        columns[1] = column2;
        columns[2] = column3;
        columns[3] = column4;
        table = new Table("database", "testTable", columns);
    }

    @Test
    public void A_testInsert() {
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
            System.out.println(e.toString());
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

        try {
            //在这里把tree保存一下
            table.serialize_tree();
        } catch (Exception e) {
            System.out.println(e.toString());
            fail();
        }

    }

    @Test
    public void B_testClose() {
        try {
            table.close();
            table.deserialize_tree(tableName);
            assertEquals(table.tableName, tableName);
            assertEquals(table.index.size(), 4);
        } catch (Exception e) {
            System.out.println(e.toString());
            fail();
        }
    }

    @Test
    public void C_testRecover() {
        try {
            Row data1_test = table.getRowFromFile(new Entry(1));
            assertEquals(data1_test.get(0), (Integer)1);
            assertEquals(data1_test.get(1), (String)"Zhang");
            assertEquals(data1_test.get(2), (double)1.1);
            assertEquals(data1_test.get(3), (long)111);

            Row data3_test = table.getRowFromFile(new Entry(3));
            assertEquals(data3_test.get(0), (Integer)3);
            assertEquals(data3_test.get(1), null);
            assertEquals(data3_test.get(2), (double)3.3);
            assertEquals(data3_test.get(3), (long)3333);

            Row data4_test = table.getRowFromFile(new Entry(4));
            assertEquals(data4_test.get(0), (Integer)4);
            assertEquals(data4_test.get(1), (String)"Yi Dong Kai Fa");
            assertEquals(data4_test.get(2), (double)4.4);
            assertEquals(data4_test.get(3), null);
        } catch (Exception e) {
            //不应该抛出异常
            System.out.println(e.toString());
            fail();
        }
    }

    @Test
    public void D_testUpdate() {
        try {
            LinkedList data6 = new LinkedList();
            data6.add(new Integer(6));
            data6.add(new String("Ruan Jian Fen Xi"));
            data6.add(new Double(6.6));
            data6.add(null);
            table.insert(data6);

            LinkedList new_data = new LinkedList();
            new_data.add(new Integer(7));
            new_data.add(new String("new Ruan Jian Fen Xi"));
            new_data.add(new Double(7.7));
            new_data.add(new Long(777));
            table.update(data6, new_data);

            Row newData_test = table.getRowFromFile(new Entry(7));
            assertEquals(newData_test.get(0), (Integer)7);
            assertEquals(newData_test.get(1), (String)"new Ruan Jian Fen Xi");
            assertEquals(newData_test.get(2), (double)7.7);
            assertEquals(newData_test.get(3), (long)777);

        } catch (Exception e) {
            //这里不应该抛出异常
            System.out.println(e.toString());
            fail();
        }
    }

    @Test
    public void E_testDelete() {
        try {
            LinkedList data8 = new LinkedList();
            data8.add(new Integer(8));
            data8.add(new String("Ren Gong Zhi Neng"));
            data8.add(new Double(8.8));
            data8.add(new Long(88));
            table.insert(data8);
            table.delete(data8);

        } catch (Exception e) {
            //这里不应该抛出异常
            fail();
        }
    }
}
