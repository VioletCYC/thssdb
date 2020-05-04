package cn.edu.thssdb.exception;

//created by cyc
public class NameNotExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: this name doesn't exist!";
    }
}
