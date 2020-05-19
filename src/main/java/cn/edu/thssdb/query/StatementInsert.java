package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;
import java.util.LinkedList;

public class StatementInsert {
    private String table_name;
    private LinkedList values;

    public StatementInsert(String table_name, LinkedList value){
        this.table_name = table_name;
        this.values = value;
    }

    public void exec(Database db){
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        db.insert(table_name, values);
    }
}
