package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class IServiceHandler implements IService.Iface {

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    ConnectResp resp = new ConnectResp();
    resp.setSessionId(Global.SessionID);
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    // TODO
    DisconnectResp resp = new DisconnectResp();
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if(req.getSessionId() != Global.SessionID)
      throw new NDException("Please first connect to database!");

    //TODO: 处理传来的字符串 req_text，执行完后得到 ExecResult，根据内容创建 ExecuteStatementResp对象返回
    String req_text = req.getStatement();
    System.out.println(req_text);

    ThssDB thssDB = ThssDB.getInstance();
    Database db = thssDB.getDatabase();
    String name = "lol";
    thssDB.createDatabase(name);
    thssDB.deleteDatabase(name);
    thssDB.switchDatabase(name);

    
    //TODO： 示例
    ExecuteStatementResp resp = new ExecuteStatementResp();
    List<String> data = new ArrayList<String>();
    data.add("ID");
    data.add("name");
    resp.setColumnsList(data);
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }
}
