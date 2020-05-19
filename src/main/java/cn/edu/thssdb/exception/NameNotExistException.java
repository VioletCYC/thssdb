package cn.edu.thssdb.exception;

import cn.edu.thssdb.schema.Column;

//created by cyc
public class NameNotExistException extends RuntimeException {
    private int errorType;
    public static int TableName = 1;
    public static int DatabaseName = 2;
    public static int ColumnName = 3;

    public NameNotExistException(int type){errorType = type;}

    @Override
    public String getMessage() {
        String msg = "";
        if(errorType == TableName)
            msg = "Exception: table name does not exist!";
        else if(errorType == DatabaseName)
            msg = "Exception: database name does not exist!";
        else if(errorType == ColumnName)
            msg = "Exception: column name does not exist!";
        return msg;
    }
}
