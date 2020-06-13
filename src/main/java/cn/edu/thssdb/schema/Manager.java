package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.FileException;
import cn.edu.thssdb.exception.NameAlreadyExistException;
import cn.edu.thssdb.exception.NameNotExistException;
import cn.edu.thssdb.server.ThssDB;

import java.io.*;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  public HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private static String manager_file = "data/manager.ma";
  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    //如果目录存不存在，则先创建目录
    File dir = new File("data/");
    if(!dir.exists())
      dir.mkdirs();

    //创建或打开manager文件，读入数据库hashmap
    databases = new HashMap<String, Database>();
    File f = new File(manager_file);
    if(!f.exists()){
      try{
        boolean sign = f.createNewFile();
        if(!sign)
          throw new FileException(FileException.CreateError);
      }
      catch (Exception e){
        throw new FileException(FileException.CreateError);
      }
    }
    else{
      try{
        BufferedReader reader = new BufferedReader(new FileReader(f));
        String str;
        while((str=reader.readLine())!=null) {
          Database db = new Database(str);
          databases.put(str, db);
        }
      }
      catch (Exception e){
        e.printStackTrace();
        throw new FileException(FileException.IOError);
      }
    }

  }

  public Database createDatabaseIfNotExists(String dbname) {
    // TODO
    //名字存在抛出错误，否则创建database
    if(this.databases.containsKey(dbname))
      throw new NameAlreadyExistException(NameAlreadyExistException.DatabaseName);

    Database new_db = new Database(dbname);
    this.databases.put(dbname, new_db);

    persist();
    return new_db;
  }

  public void deleteDatabase(String dbname) {
    // TODO
    //名字不存在抛出错误，否则删除database相应文件夹并删去hashmap键值对
    if(!this.databases.containsKey(dbname))
      throw new NameNotExistException(NameNotExistException.DatabaseName);

    this.databases.remove(dbname);
    File f = new File("data/" + dbname + "/");
    boolean sign = deleteDir(f);
    if(!sign)
      throw new FileException(FileException.DeleteError);


    persist();
  }

  public static boolean deleteDir(File someFile) {
    if (!someFile.exists())
      return false;

    if (someFile.isDirectory()) {// is a folder
      File[] files = someFile.listFiles();
      for (File subFile : files) {
        boolean isSuccess = subFile.delete();
        System.out.println(subFile.getName()+isSuccess);
        if(!isSuccess)
          return false;
      }
    }

    boolean isSuccess = someFile.delete();
    return isSuccess;
  }


  public Database switchDatabase(String dbname) {
    // TODO
    if(!databases.containsKey(dbname))
      throw new NameNotExistException(NameNotExistException.DatabaseName);

    persist();
    return databases.get(dbname);
  }

  public void persist(){
    //将hashmap写到文件中
    File f = new File(manager_file);
    if(!f.exists()){
      try{
        boolean sign = f.createNewFile();
        if(!sign)
          throw new FileException(FileException.CreateError);
      }
      catch (Exception e){
        throw new FileException(FileException.CreateError);
      }
    }

    try {
      FileWriter writer = new FileWriter(f);
      Set<String> set = databases.keySet();
      for (String key : set) {
        databases.get(key).quit();
        writer.write(key + "\n");
      }
      writer.close();
    }
    catch (Exception e){
      e.printStackTrace();
      throw new FileException(FileException.IOError);
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
