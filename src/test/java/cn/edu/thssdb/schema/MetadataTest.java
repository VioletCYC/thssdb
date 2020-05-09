package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.NameAlreadyExistException;
import cn.edu.thssdb.exception.NameNotExistException;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runners.MethodSorters;

import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MetadataTest {
    private Table table;
    private Database database;
    private Manager manager;
    private String dbName = "test";
    private String tableName = "testTable";
    private Column[] columns;

    @BeforeEach
    public void setUp()
            throws IOException, ClassNotFoundException {
        manager = Manager.getInstance();
    }

    @Test
    public void Test1() {
        database = manager.createDatabaseIfNotExists(dbName);
        Column column1 = new Column("column1", ColumnType.INT, 1, true, 2);
        Column column2 = new Column("column2", ColumnType.STRING, 0, false, 30);
        Column column3 = new Column("column3", ColumnType.DOUBLE, 0, true, 5);
        Column column4 = new Column("column4", ColumnType.LONG, 0, false, 4);

        columns = new Column[4];
        columns[0] = column1;
        columns[1] = column2;
        columns[2] = column3;
        columns[3] = column4;
        database.create(tableName, columns);
        database.create("exist", columns);

        Column[] col = new Column[0];
        assertThrows(NameAlreadyExistException.class, ()->database.create("exist", col));
        assertDoesNotThrow(()->database.create("newtable", col));
        assertThrows(NameNotExistException.class, ()->database.drop("null"));
        assertDoesNotThrow(()->database.drop("newtable"));
        File f = new File("data/"+dbName+"/"+"newtable.data");
        assertFalse(f.exists());
        database.quit();
    }

    @Test
    public void Test2() {
        assertDoesNotThrow(()->manager.createDatabaseIfNotExists("1"));
        assertThrows(NameNotExistException.class, ()->manager.deleteDatabase("2"));
        assertThrows(NameAlreadyExistException.class, ()->manager.createDatabaseIfNotExists("1"));
        Database op = manager.switchDatabase("test");
        op.quit();
        //assertDoesNotThrow(()->manager.deleteDatabase("test"));
    }

}
