package cn.edu.thssdb.transaction;

import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.exception.NullPointerException;

import java.util.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;


public class TransactionManager2PL{
    public Database db;
    private HashMap<String, Session> tableWriteLock;
    private HashMap<String, Session> tableReadLock;
    public static int READ_COMMITED = 1;
    public static int REPEATABLE_READ = 2;
    public static int SERIALIZABLE = 3;
    private int isolation;

    public TransactionManager2PL(Database db, int level){
        this.db = db;
        this.tableWriteLock = new HashMap();
        this.isolation = level;
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
        else if(!session.isAbort){
            setWaitingSession(session);
        }
        db.lock.writeLock().unlock();

        if(session.isAbort)
            commit(session);
    }


    //逐条执行session中的语句
    public void beginExecution(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        if(session.isAbort)
            return;

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
                else if(session.statement.get(i) instanceof  StatementSelect){
                    StatementSelect cs = (StatementSelect) session.statement.get(i);
                    addSelect(session, cs);
                }
            }
            catch (Exception e){
                session.isAbort = true;
                rollback(session);
                break;
            }
        }

        commit(session);
    }

    //判断该session要读写的表是否正在被别的session锁住
    //TODO:死锁判断
    private boolean setWaitedSession(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        session.temp.clear();
        for(String s: session.TableForWrite){
            Session holder = tableWriteLock.get(s);
            if(holder!=null && holder!=session){
                session.temp.add(holder);
            }

            if(isolation > READ_COMMITED) {
                holder = tableReadLock.get(s);
                if (holder != null && holder != session) {
                    session.temp.add(holder);
                }
            }
        }

        if(isolation > READ_COMMITED) {
            for (String s : session.TableForRead) {
                Session holder = tableWriteLock.get(s);
                if (holder != null && holder != session) {
                    session.temp.add(holder);
                }
            }
        }

        if(session.temp.isEmpty())
            return true;

        if(!checkDeadLock(session, session.temp))
            session.isAbort = true;

        return false;
    }

    private boolean checkDeadLock(Session session, LinkedHashSet newWait){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        int count = session.waitingSession.size();
        Iterator<Session> it = session.waitingSession.iterator();
        while(it.hasNext()){
            Session current = it.next();
            if(newWait.contains(current))
                return false;
            if(!checkDeadLock(current, newWait))
                return false;
        }

        return true;
    }

    //将该session正在等待的目标session加入目标session中
    private void setWaitingSession(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        for(Session s: session.temp){
            s.waitingSession.add(session);
        }
        session.temp.clear();
    }

    //事务语句执行前，给要读写的表上锁
    private void lockTables(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        ArrayList<String> TableToWrite = session.getTableForWrite();
        for(String s: TableToWrite){
            this.tableWriteLock.put(s, session);
        }

        if(this.isolation > READ_COMMITED){
            ArrayList<String> TableToRead = session.getTableForRead();
            for(String s: TableToRead){
                this.tableReadLock.put(s, session);
            }
        }

    }


    //持久化存储，并释放所有锁
    public LinkedList<ExecResult> commit(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        db.lock.writeLock().lock();
        //持久化存储写过的表
        if(!session.isAbort) {
            for (String s : session.TableForWrite) {
                try {
                    db.getTable(s).persist();
                } catch (Exception e) {
                    rollback(session);
                }
            }
        }
        unlockTables(session);
        db.lock.writeLock().unlock();


    }

    //commit后，释放所有锁
    private void unlockTables(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

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
        resetlock(session);
    }

    private void resetlock(Session session){
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        int count = session.waitingSession.size();
        if(count <= 0)
            return;

        Iterator<Session> it = session.waitingSession.iterator();
        while(it.hasNext()){
            Session cur = it.next();
            boolean canProceed = setWaitedSession(cur);
            if(canProceed){
                lockTables(cur);
                beginExecution(cur);
            }
            else{
                setWaitingSession(cur);
            }
        }
    }

    public void addInsert(Session session, StatementInsert cs) throws IOException {
        ExecResult res = cs.exec(db);
        String table_name = cs.gettable_name();
        RowAction action = new RowAction(table_name, 1, null, res.getNewValue());
        session.rowActionList.add(action);
    }

    public void addDelete(Session session, StatementDelete cs) throws IOException{
        ExecResult res = cs.exec(db);
        String table_name = cs.gettable_name();
        RowAction action = new RowAction(table_name, 2, res.getOldValue(), null);
        session.rowActionList.add(action);
    }

    //TODO
    public void addUpdate(Session session, StatementUpdate cs){
        try{
            ExecResult res = cs.exec(db);
            String table_name = cs.gettable_name();
            RowAction action = new RowAction(table_name, 3, res.getOldValue(), res.getNewValue());
            session.rowActionList.add(action);
        }
        catch (Exception e){}
    }

    //TODO
    public void addSelect(Session session, StatementSelect cs){
        try{
            ExecResult res = cs.exec(db);
            unlockReadLock(session, cs);
            session.result.add(res);
        }
        catch (Exception e){}
    }

    private void unlockReadLock(Session session, StatementSelect cs){
        if(isolation == REPEATABLE_READ) {
            LinkedList<String> table_list = cs.getTargetList();
            for (String s : table_list) {
                tableReadLock.remove(s);
            }
        }
    }

    //TODO：回滚具体操作，后续完成
    private void rollback(Session session){
        db.lock.writeLock().lock();
        int count = session.rowActionList.size();
        for(int i=count-1; i>=0; i++){
            RowAction tem = session.rowActionList.get(i);
            if(tem.getType()==1){
                LinkedList<LinkedList> newRow = tem.getNewRow();
                for(LinkedList row: newRow){

                }
            }
        }

        db.lock.writeLock().unlock();
    }

    public void setIsolation(int level){isolation=level;}
}