package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;

public class StatementDropTable {
    private String table_name;

    public StatementDropTable(String table_name){
        this.table_name = table_name;
    }

    public void Exec(Database db){
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        db.drop(table_name);
    }
}

