package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.transaction.TransactionManager2PL;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;

public class ThssDB {

  private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

  private static IServiceHandler handler;
  private static IService.Processor processor;
  private static TServerSocket transport;
  private static TServer server;

  private Manager manager;
  private TransactionManager2PL transactionManager;
  private Database database;

  public static ThssDB getInstance() {
    return ThssDBHolder.INSTANCE;
  }

  private ThssDB() {
    manager = new Manager();
  }

  public static void main(String[] args) {
    ThssDB server = ThssDB.getInstance();
//    Manager manager = new Manager();
    server.start();
  }

  private void start() {
    handler = new IServiceHandler();
    processor = new IService.Processor(handler);
    Runnable setup = () -> setUp(processor);
    new Thread(setup).start();
  }

  private static void setUp(IService.Processor processor) {
    try {
      transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
      server = new TSimpleServer(new TServer.Args(transport).processor(processor));
      logger.info("Starting ThssDB ...");
      server.serve();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  public Database createDatabase(String name) {
    database = manager.createDatabaseIfNotExists(name);
    return database;
  }

  public Database switchDatabase(String name){
    database = manager.switchDatabase(name);
    return manager.switchDatabase(name);
  }

  public void deleteDatabase(String name){
    manager.deleteDatabase(name);
  }

  public Database getDatabase() {
    return database;
  }

  private static class ThssDBHolder {
    private static final ThssDB INSTANCE = new ThssDB();
    private ThssDBHolder() {

    }
  }
}
