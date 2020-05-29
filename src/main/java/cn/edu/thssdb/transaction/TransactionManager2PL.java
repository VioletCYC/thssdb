package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Database;

import java.util.HashMap;
import java.util.Iterator;

public class TransactionManager2PL{
    public Database db;
    private HashMap tableWriteLock;

    public TransactionManager2PL(Database db){
        this.db = db;
        this.tableWriteLock = new HashMap();
    }

    public void beginTransaction(Session session){
        session.inTransaction = true;
        db.lock.writeLock().lock();

    }

    private boolean setWaitedSession(Session session){
        //TODO
        return true;
    }

    private void lockTables(Session session){


    }

}