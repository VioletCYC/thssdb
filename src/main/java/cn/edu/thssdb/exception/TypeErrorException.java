package cn.edu.thssdb.exception;

public class TypeErrorException extends RuntimeException{
    private int errortype;
    public static int Conditions = 1;
    public static int Expression = 2;
    public static int NotNumber = 3;
    public static int NotBaseType = 4;
    private String extra;

    public TypeErrorException(String extra, int type) {
        this.extra = extra;
        errortype = type;
    }

    @Override
    public String getMessage() {
        String msg = "";
        if (errortype == Conditions)
            msg = "Exception: Unexpected condition type!";
        else if(errortype == Expression)
            msg = "Exception: Unexpected expression type!";
        else if(errortype == NotNumber)
            msg = "Exception:" + this.extra + " is not a valid number!";
        else if(errortype == NotBaseType)
            msg = "Exception:" + this.extra + " , not basic type!";

        return msg;

    }
}
