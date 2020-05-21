package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.GrammarException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.LinkedList;

public class StatementInsert {
    private String table_name;
    private LinkedList<LinkedList> values;
    private LinkedList<String> col;

    public StatementInsert(String table_name, LinkedList<LinkedList> value){
        this.table_name = table_name;
        this.values = value;
    }

    public StatementInsert(String table_name, LinkedList<String> col, LinkedList<LinkedList> value){
        this.table_name = table_name;
        this.col = col;
        this.values = value;
    }

    public ExecResult exec(Database db){
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        Table targetTable = db.getTable(table_name);
        if (this.col != null) {
            LinkedList<LinkedList> valueList = this.values;
            this.values = new LinkedList<>();
            ArrayList<String> colNames = targetTable.getColNames();
            for (int i = 0; i < valueList.size(); ++i) {
                LinkedList newCol = new LinkedList();
                for (int j = 0; j < colNames.size(); ++j)
                    newCol.add(null);
                this.values.add(newCol);
            }


            for (int j = 0; j < this.col.size(); ++j) {
                String attr = this.col.get(j);
                int idx = colNames.indexOf(attr);
                if (idx >= 0) {
                    for (int i = 0; i < valueList.size(); ++i) {
                        LinkedList values = this.values.get(i);
                        values.set(idx, valueList.get(i).get(j));
                    }
                } else {
                    throw new GrammarException("Unknown column name '" + attr + "'!");
                }
            }
        }

//      LinkedList<String> tableHeader = new LinkedList<>();
//      tableHeader.add("Update_Count");
//      ExecResult execResult = new ExecResult(tableHeader);
        int succeed = 0;
//        if ((int)this.valueList.get(0).get(2) == 5844) {
//            System.out.println(this.valueList.get(0));
//        }
        for (LinkedList value: this.values) {
            try{
                targetTable.insert(value);
                succeed += 1;
            }
            catch(Exception e){}
        }
//        targetTable.close();

//        LinkedList val = new LinkedList();
//        val.add(succeed);
//        execResult.insert(val);
        return new ExecResult("insert " + succeed + " rows!");
    }
}
