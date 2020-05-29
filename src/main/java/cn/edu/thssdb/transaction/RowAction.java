package cn.edu.thssdb.transaction;

import cn.edu.thssdb.schema.Row;

import java.util.LinkedList;

public class RowAction {
    private LinkedList oldRow;
    private LinkedList newRow;
    private String table_name;

    public RowAction(String table_name, LinkedList oldRow, LinkedList newRow){
        this.table_name = table_name;
        this.oldRow = oldRow;
        this.newRow = newRow;
    }

}
