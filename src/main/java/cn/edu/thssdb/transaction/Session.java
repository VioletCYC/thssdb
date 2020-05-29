package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Database;

import java.util.LinkedList;

public class Session{
    private Database db;
    private int ID;
    private LinkedList<RowAction> rowActionList;
    public static int READ_COMMITED = 1;
    public static int REPEATABLE_READ = 2;
    public static int SERIALIZABLE = 3;
    public boolean inTransaction = false;
    public LinkedList<String> TableForWrite;

    public Session(Database db, int id){
        this.db = db;
        this.ID = id;
        this.rowActionList = new LinkedList<>();
    }

    public void AddInsert(String table_name){
        if(!TableForWrite.contains(table_name))
            TableForWrite.add(table_name);
    }



    public void setIsolation(int level){

    }

}
