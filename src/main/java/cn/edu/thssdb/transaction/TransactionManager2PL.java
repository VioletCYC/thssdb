package cn.edu.thssdb.transaction;

import cn.edu.thssdb.query.ExecResult;
import cn.edu.thssdb.query.StatementDelete;
import cn.edu.thssdb.query.StatementInsert;
import cn.edu.thssdb.query.StatementUpdate;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class TransactionManager2PL{
    public Database db;
    private HashMap<String, Session> tableWriteLock;
    private HashMap<String, Session> tableReadLock;

    public TransactionManager2PL(Database db){
        this.db = db;
        this.tableWriteLock = new HashMap();
    }

    //根据锁判断是否能进行该session的一系列操作
    public void beginTransaction(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        db.lock.writeLock().lock();
        boolean canProceed = setWaitedSession(session);
        if(canProceed){
            session.inTransaction = true;
            lockTables(session);
            beginExecution(session);
        }
        else{
            setWaitingSession(session);
        }
        db.lock.writeLock().unlock();

    }

    //需要的锁都已释放，开始此事务
    public void beginActionResume(Session session){

    }

    //逐条执行session中的语句
    public void beginExecution(Session session){
        int count = session.statement.size();
        for(int i=0; i<count; i++){
            try {
                if (session.statement.get(i) instanceof StatementInsert) {
                    StatementInsert cs = (StatementInsert) session.statement.get(i);
                    addInsert(session, cs);
                }
                else if(session.statement.get(i) instanceof  StatementDelete){
                    StatementDelete cs = (StatementDelete) session.statement.get(i);
                    addDelete(session, cs);
                }
                else if(session.statement.get(i) instanceof  StatementUpdate){
                    StatementUpdate cs = (StatementUpdate) session.statement.get(i);
                    addUpdate(session, cs);
                }
            }
            catch (Exception e){

            }
        }
    }

    //判断该session要读写的表是否正在被别的session锁住
    //TODO:死锁判断
    private boolean setWaitedSession(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);


        for(String s: session.TableForWrite){
            Session holder = tableWriteLock.get(s);
            if(holder!=null && holder!=session){
                session.temp.add(holder);
            }
            holder = tableReadLock.get(s);
            if(holder!=null && holder!=session){
                session.temp.add(holder);
            }

        }

        for(String s: session.TableForRead){
            Session holder = tableWriteLock.get(s);
            if(holder!=null && holder!=session){
                session.temp.add(holder);
            }
        }

        return session.temp.isEmpty();
    }

    //将该session正在等待的目标session加入目标session中
    private void setWaitingSession(Session session){
        for(Session s: session.temp){
            s.waitingSession.add(session);
        }
        session.temp.clear();
    }

    //事务语句执行前，给要读写的表上锁
    private void lockTables(Session session){
        ArrayList<String> TableToWrite = session.getTableForWrite();
        for(String s: TableToWrite){
            this.tableWriteLock.put(s, session);
        }

        ArrayList<String> TableToRead = session.getTableForRead();
        for(String s: TableToRead){
            this.tableReadLock.put(s, session);
        }

    }

    //commit后，释放所有锁
    private void unlockTables(Session session){
        Iterator it = tableWriteLock.values().iterator();
        while (it.hasNext()) {
            Session s = (Session) it.next();
            if (s == session) {
                it.remove();
            }
        }

        it = tableReadLock.values().iterator();
        while (it.hasNext()) {
            Session s = (Session) it.next();
            if (s == session) {
                it.remove();
            }
        }
    }

    //持久化存储，并释放所有锁
    public void commit(Session session){

        //持久化存储写过的表
        for(String s: session.TableForWrite){
            try{
                db.getTable(s).persist();
            }
            catch (Exception e){
                rollback(session, session.statement.size());
            }
        }
        unlockTables(session);
    }

    public void addInsert(Session session, StatementInsert cs) throws IOException {
        ExecResult res = cs.exec(db);
        String table_name = cs.gettable_name();
        RowAction action = new RowAction(table_name, 1, null, res.getNewValue());
        session.rowActionList.add(action);

        endEachAction(session);
    }

    public void addDelete(Session session, StatementDelete cs) throws IOException{
        ExecResult res = cs.exec(db);
        String table_name = cs.gettable_name();
        RowAction action = new RowAction(table_name, 2, res.getOldValue(), null);
        session.rowActionList.add(action);

        endEachAction(session);
    }

    public void addUpdate(Session session, StatementUpdate cs){

    }

    private void endEachAction(Session session){

    }

    private void rollback(Session session, int index){

    }

}