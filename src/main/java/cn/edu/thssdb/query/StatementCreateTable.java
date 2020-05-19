package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;
import cn.edu.thssdb.exception.NullPointerException;
import java.util.ArrayList;

public class StatementCreateTable {
    private String table_name;
    private Column[] cols;

    public StatementCreateTable(String table_name,
                                Column[] cols){
        this.table_name = table_name;
        this.cols = cols;
    }

    public void exec(Database db){
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        db.create(table_name, cols);
    }
}


