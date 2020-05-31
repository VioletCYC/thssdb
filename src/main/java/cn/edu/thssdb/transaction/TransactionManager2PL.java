package cn.edu.thssdb.transaction;

import cn.edu.thssdb.exception.NullPointerException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;



public class TransactionManager2PL{
    public Database db;
    private HashMap<String, Session> tableWriteLock;
    private HashMap<String, Session> tableReadLock;

    public TransactionManager2PL(Database db){
        this.db = db;
        this.tableWriteLock = new HashMap();
    }

    //根据锁判断是否能进行该session的一系列操作
    public void beginTransaction(Session session) throws IOException, ClassNotFoundException {
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

    }


    //逐条执行session中的语句
    public void beginExecution(Session session) throws IOException, ClassNotFoundException {
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        if(session.isAbort)
            return;

        if (session.f.exists()) {
            //session.f.createNewFile();
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(session.f));
            while(ois.readObject()!=null) {
                String tablename = (String) ois.readObject();
                int type = (int) ois.readObject();
                LinkedList<LinkedList> oldvalue = (LinkedList<LinkedList>) ois.readObject();
                LinkedList<LinkedList> newvalue = (LinkedList<LinkedList>) ois.readObject();
                ois.close();
                Table table = db.getTable(tablename);
                if (type == 1) {
                    table.insert(newvalue);
                }
                if (type == 2) {
                    Entry key = table.getKey(oldvalue);
                    table.delete(key);
                }
                if (type == 3) {
                    table.insert(newvalue);
                    Entry key = table.getKey(oldvalue);
                    table.delete(key);
                }
            }
            session.f.delete();

        }
        else {

        }

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
                session.isAbort = true;
                rollback(session);
            }
            session.f.delete();
        }
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

        ArrayList<String> TableToRead = session.getTableForRead();
        for(String s: TableToRead){
            this.tableReadLock.put(s, session);
        }

    }


    //持久化存储，并释放所有锁
    public void commit(Session session) throws IOException, ClassNotFoundException {
        if(session == null)
            throw new NullPointerException(NullPointerException.Session);

        db.lock.writeLock().lock();
        //持久化存储写过的表
        for(String s: session.TableForWrite){
            try{
                db.getTable(s).persist();
            }
            catch (Exception e){
                rollback(session);
            }
        }
        unlockTables(session);
        db.lock.writeLock().unlock();
    }

    //commit后，释放所有锁
    private void unlockTables(Session session) throws IOException, ClassNotFoundException {
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

    private void resetlock(Session session) throws IOException, ClassNotFoundException {
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
        }
        catch (Exception e){}
    }

    private void unlockReadLock(Session session, StatementSelect cs){
        if(session.getIsolation() == 2)
            return;

        LinkedList<String> table_list = cs.getTargetList();
        for(String s: table_list){
            tableReadLock.remove(s);
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
            }
        }

        db.lock.writeLock().unlock();
    }
/*
    public void writeInsert(Session session, StatementInsert cs) throws IOException {
        Writer out =new FileWriter(session.f);

    }
*/
}
