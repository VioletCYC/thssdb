package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Database;

public abstract class AbstractStatement {
    abstract public ExecResult exec(Database db);
}
