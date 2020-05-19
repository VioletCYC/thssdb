package cn.edu.thssdb.exception;

public class NullPointerException extends RuntimeException {
    private int errorType;
    public static int Table = 1;
    public static int Database = 2;
    public static int Column = 3;

    public NullPointerException(int type){errorType = type;}

    @Override
    public String getMessage() {
        String msg = "";
        if(errorType == Database)
            msg = "Exception: Null database pointer!";

        return msg;
    }
}
