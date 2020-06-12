package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.transaction.Session;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;

public class StatementDelete extends AbstractStatement{
    private String table_name;
    private Conditions cond;
    private Session session;

    public StatementDelete(String table_name, Conditions cond){
        this.table_name = table_name;
        this.cond = cond;
    }

    public StatementDelete(String table_name){
        this.table_name = table_name;
    }


    //执行可能多行的删除
    @Override
    public ExecResult exec(Database db)
            throws IOException {
        if(db == null)
            throw new NullPointerException(NullPointerException.Database);

        Table targetTable = db.getTable(this.table_name);
        ArrayList<Table> param = new ArrayList<>();
        param.add(targetTable);

        //这里不需要正则化：无需ID->PERSON.ID
//        if (cond != null)
//            cond.normalize(param);

        LinkedList<LinkedList> allRow = new LinkedList<>();
        int succeed = 0;
        ArrayList<Entry> rowList = targetTable.search(cond);
        for (Entry key: rowList) {
            LinkedList row = targetTable.getRowAsList(key);
            targetTable.delete(key);
            succeed += 1;
            allRow.add(row);
        }
//        targetTable.close();

        // TODO: 写log
        // for log
//        LinkedList<LinkedList> oldvalue = allRow;
//        LinkedList<LinkedList> newvalue = null;
//        FileOutputStream fileInputStream = new FileOutputStream(session.f);
//        ObjectOutputStream oos = new ObjectOutputStream(fileInputStream);
//        oos.writeObject(this.table_name);
//        oos.writeObject(2);
//        oos.writeObject((oldvalue));
//        oos.writeObject(newvalue);
//        oos.close();
        //

        return new ExecResult("Delete " + succeed + " rows.", 2, allRow, null);
    }

    public String gettable_name(){return this.table_name;}
}
