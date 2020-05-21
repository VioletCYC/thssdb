package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.LinkedList;

public class StatementDelete {
    private String table_name;
    private Conditions cond;

    public StatementDelete(String table_name, Conditions cond){
        this.table_name = table_name;
        this.cond = cond;
    }

    public StatementDelete(String table_name){
        this.table_name = table_name;
    }

    public ExecResult exec(Database db){
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        Table targetTable = db.getTable(this.table_name);
        ArrayList<Table> param = new ArrayList<>();
        param.add(targetTable);
        if (cond != null)
            cond.normalize(param);

        int succeed = 0;
        try{
            ArrayList<Entry> rowList = targetTable.search(cond);
            for (Entry key: rowList) {
                targetTable.delete(key);
                succeed += 1;
            }
        }
        catch (Exception e){

        }
//        targetTable.close();

        return new ExecResult("Delete " + succeed + " rows.");
    }
}
