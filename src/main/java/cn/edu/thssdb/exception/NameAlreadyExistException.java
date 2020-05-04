package cn.edu.thssdb.exception;

//created by cyc
public class NameAlreadyExistException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: this name already exists!";
    }
}
