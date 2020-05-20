package cn.edu.thssdb.query;

public class StatementDelete {
    private String table_name;
    private Conditions cond;

    public StatementDelete(String table_name, Conditions cond){
        this.table_name = table_name;
        this.cond = cond;
    }

    public StatementDelete(String table_name){
        this.table_name = table_name;
    }
}
