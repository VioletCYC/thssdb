package cn.edu.thssdb.type;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.exception.TypeErrorException;

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

  public static Object convert(String str, ColumnType type){
    switch(type){
      case INT:
        return Integer.parseInt(str);
      case LONG:
        return Long.parseLong(str);
      case FLOAT:
        return Float.parseFloat(str);
      case DOUBLE:
        return Double.parseDouble(str);
      case STRING:
        return str;
      default:
        throw new TypeErrorException("nouse", TypeErrorException.NotBaseType);
    }
  }

  //根据计算的两数类型，得出结果的数据类型
  public static ColumnType lift(ColumnType t1, ColumnType t2) {
    if ((isNumber(t1) && !isNumber(t2)) || (!isNumber(t1) && isNumber(t2)))
      throw new TypeErrorException("Type " + t1+ " & " + t2, TypeErrorException.NotBaseType);
    if (!isNumber(t1) && !isNumber(t2))
      return STRING;
    if (t1 == DOUBLE || t2 == DOUBLE)
      return DOUBLE;
    if (t1 == FLOAT || t2 == FLOAT)
      return FLOAT;
    if (t1 == LONG || t2 == LONG)
      return LONG;
    return INT;
  }

  private static Boolean isNumber(ColumnType type){
    if(type==INT || type==DOUBLE || type==LONG || type==FLOAT)
      return true;

    return false;
  }

};

