package cn.edu.thssdb.transaction;

import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Database;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;


//对于每个事务创建一个Session对象
//一个事务应当整体读入（从begin transaction开始到commit）
//对于事务中的每条sql语句，逐条调用 AddInsert, AddDelete, AddUpdate, AddSelect
// commit时调用transactionmanager2PL中的接口开始执行
//事务中允许的语句只有 insert, delete, update, select

public class Session{
    private Database db;
    private int ID;
    public LinkedList<RowAction> rowActionList;
    public ArrayList<String> TableForWrite;
    public ArrayList<String> TableForRead;
    public boolean inTransaction = false;
    public boolean isAbort = false;
    public LinkedHashSet<Session> temp;                //该session正在等待哪些session
    public LinkedHashSet<Session> waitingSession;      //有哪些session在等待该session结束
    public ArrayList<AbstractStatement> statement;
    public LinkedList<ExecResult> result;
    private String prefix = "log/";
    public  File f;

    public Session(Database db, int id) throws IOException, ClassNotFoundException {
        this.db = db;
        this.ID = id;
        this.rowActionList = new LinkedList<>();
        this.TableForWrite = new ArrayList<>();
        this.TableForRead = new ArrayList<>();
        this.result = new LinkedList<>();
        this.f = new File(prefix + id +".log");
    }

    public void AddInsert(StatementInsert cs){
        String table_name = cs.gettable_name();
        if(!TableForWrite.contains(table_name))
            TableForWrite.add(table_name);

        statement.add(cs);
    }

    public void AddDelete(StatementDelete cs){
        String table_name = cs.gettable_name();
        if(!TableForWrite.contains(table_name))
            TableForWrite.add(table_name);

        statement.add(cs);
    }

    public void AddUpdate(StatementUpdate cs){
        String table_name = cs.gettable_name();
        if(!TableForWrite.contains(table_name))
            TableForWrite.add(table_name);

        statement.add(cs);
    }

    public void AddSelect(StatementSelect cs){
        LinkedList<String> table_list = cs.getTargetList();
        for(String name: table_list){
            if(!TableForRead.contains(name)){
                TableForRead.add(name);
            }
        }

        statement.add(cs);
    }


    public ArrayList<String> getTableForWrite(){return this.TableForWrite;}

    public ArrayList<String> getTableForRead(){return this.TableForRead;}
}
