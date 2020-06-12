package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.parser.MyVisitor;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.query.AbstractStatement;
import cn.edu.thssdb.query.ExecResult;
import cn.edu.thssdb.query.StatementDatabase;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.query.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;

import java.nio.charset.Charset;
import java.util.*;

public class IServiceHandler implements IService.Iface {

  Database database;
  ThssDB server = ThssDB.getInstance();

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
    if (req.getSessionId() != Global.SessionID)
      throw new NDException("Please first connect to database!");

    //TODO: 处理传来的字符串 req_text，执行完后得到 ExecResult，根据内容创建 ExecuteStatementResp对象返回
    String req_text = req.getStatement();
    System.out.println(req_text);

    ExecuteStatementResp resp = new ExecuteStatementResp();
    resp.setResultInfo(new LinkedList<>());
    //开始解析
    CharStream stream = CharStreams.fromString(req_text);
    SQLLexer lexer = new SQLLexer(stream);
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    SQLParser parser = new SQLParser(tokenStream);
    MyVisitor visitor = new MyVisitor();
    ArrayList res = (ArrayList) visitor.visit(parser.parse());
    for (Object statement : res) {
      try {
        //如果是操作数据库的语句：直接送到ThssDB里执行
        if (statement instanceof StatementDatabase) {
          switch (((StatementDatabase) statement).type) {
            case 1:
              database = server.createDatabase(((StatementDatabase) statement).db);
              break;
            case 2:
              database = server.switchDatabase(((StatementDatabase) statement).db);
              break;
            case 3:
              server.deleteDatabase(((StatementDatabase) statement).db);
              break;
            default:
              break;
          }
        }
        else {
          ExecResult result = ((AbstractStatement) statement).exec(database);
          //如果有要返回的结果
          if(statement instanceof StatementSelect) {
            resp.setHasResult(true);
            resp.setColumnsList(result.getColNames());
            resp.setRowList(result.getDataListAsList());
          }
          else {
            //如果不是select：只把msg添加进去
            resp.getResultInfo().add(result.getMsg());
          }
        }
        resp.setStatus(new Status(Global.SUCCESS_CODE));

      } catch (Exception e) {
        resp.setStatus(new Status(Global.FAILURE_CODE));
        resp.setIsAbort(true);
        resp.setErrorInfo(e.toString());
        break;
      }
    }

    return resp;
  }
}
