package cn.edu.thssdb.type;

import cn.edu.thssdb.exception.InsertException;

public enum ColumnType {
  INT, LONG, FLOAT, DOUBLE, STRING;

  public static ColumnType fromStrToType(String type) {
    if(type.equals("INT"))
      return INT;
    else if(type.equals("LONG"))
      return LONG;
    else if(type.equals("FLOAT"))
      return FLOAT;
    else if(type.equals("DOUBLE"))
      return DOUBLE;
    else if(type.equals("STRING"))
      return STRING;
    else
      throw new InsertException(InsertException.TYPE_CONVERT_ERROR);
  }
};

