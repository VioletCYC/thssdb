package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.NDException;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.TempTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class StatementSelect extends AbstractStatement{
    private LinkedList<String> targetList;
    private RangeVariable rv;
    private Conditions cond;

    public StatementSelect(LinkedList<String> targetList,
                           RangeVariable rv,
                           Conditions cond) {
        this.targetList = targetList;
        this.rv = rv;
        this.cond = cond;
    }

    public ExecResult exec(Database db) throws IOException, NDException {
        if (db == null) throw new NDException("not using any database");
        ExecResult res;
        Object t = rv.exec(db);
        ArrayList<Table> tableList = rv.getTableList();
        if (t instanceof Table) {
            Table table = (Table) t;
            LinkedList<String> colNames = new LinkedList<>(table.getColNames());
//            LinkedList<Type> colTypes = new LinkedList<>(table.getColTypes());
            if (!targetList.isEmpty()) {
                if (targetList.getFirst().compareTo("*") == 0)
                    res = new ExecResult(colNames);
                else
                    res = new ExecResult(targetList);
            } else
            {
                res = new ExecResult();
            }
            colNames = table.combineTableColumn();
            tableList = new ArrayList<>();
            tableList.add(table);
            if (cond != null) {
                cond.normalize(tableList);
            }
            ArrayList<Entry> rowList = table.search(cond);
            for (Entry row: rowList) {
                LinkedList data = table.getRowAsList(row);

                if (!targetList.isEmpty()) {
                    if (targetList.getFirst().compareTo("*") == 0) {
                        res.insert(data);
                    } else
                    {
                        res.insert(colNames, data, tableList, null);
                    }
                } else
                {
                    res.insert(new LinkedList());
                }
            }
//            table.close(false);
            return res;
        } else
        {
            TempTable tempTable = (TempTable) t;
            LinkedList<String> colNames = new LinkedList<>(tempTable.getColNames());
//            LinkedList<Type> colTypes = new LinkedList<>(tempTable.getColTypes());
            LinkedList<String> ignoreColumns;
            if (rv.getIgnoredColumns() == null) {
                ignoreColumns = new LinkedList<>();
            } else {
                ignoreColumns =new LinkedList<>(rv.getIgnoredColumns());
            }
            if (!targetList.isEmpty()) {
                if (targetList.getFirst().compareTo("*") == 0)
                    res = new ExecResult(colNames, ignoreColumns);
                else
                    res = new ExecResult(targetList);
            } else
            {
                res = new ExecResult();
            }

            if (cond != null) {
                cond.normalize(tableList, ignoreColumns);
            }
            ArrayList<Entry> rowList = tempTable.search(cond);
            for (Entry row: rowList) {
                LinkedList data = tempTable.getRowFromFile(row);

                if (!targetList.isEmpty()) {
                    if (targetList.getFirst().compareTo("*") == 0) {
                        res.insert(colNames, data, ignoreColumns);
                    } else
                    {
                        res.insert(colNames, data, tableList, ignoreColumns);
                    }
                } else
                {
                    res.insert(new LinkedList());
                }
            }
            tempTable.close();
            return res;
        }
    }

    public LinkedList<String> getTargetList(){return targetList;}
}