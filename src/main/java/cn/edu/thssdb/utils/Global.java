package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;                  //同时启动多个客户端时，改变端口号
  public static int DEFAULT_SERVER_PORT2 = 6666;

  public static String CLI_PREFIX = "ThssDB>";
  public static final String SHOW_TIME = "show time;";
  public static final String QUIT = "quit;";
  public static final String CONNECT = "connect;";
  public static final String DISCONNECT = "disconnect;";
  public static long SessionID = 114514;

  public static final String S_URL_INTERNAL = "jdbc:default:connection";
}
