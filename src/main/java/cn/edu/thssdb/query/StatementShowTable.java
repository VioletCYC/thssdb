package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;

public class StatementShowTable {
    private String table_name;

    public StatementShowTable(String table_name){
        this.table_name = table_name;
    }

    public void exec(Database db){
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);


    }
}
