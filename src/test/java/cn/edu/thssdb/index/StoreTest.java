package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;

public class StoreTest {
    public static void main(String[] args) {
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
}
