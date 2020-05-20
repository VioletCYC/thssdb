package cn.edu.thssdb.query;


import java.util.ArrayList;
import java.util.LinkedList;
import java.util.function.Consumer;

public class ExecResult {
    private LinkedList<String> colNames;
    private LinkedList<LinkedList> dataList;
    private String msg;
    private int type;

    //for insert, delete, createtable, droptable, update
    public ExecResult(String msg) {
        this.msg = msg;
        this.type = 1;
    }

    //for showtable(type=2), select(type=3)
    public ExecResult(int type, LinkedList<String> colNames) {
        this.colNames = new LinkedList<>(colNames);
        this.dataList = new LinkedList<>();
    }

/*
    public ExecResult(LinkedList<String> colNames, LinkedList<String> ignoreNames) {
        this.colNames = new LinkedList<>(colNames);
        for (String ignoreName: ignoreNames) {
            this.colNames.remove(ignoreName);
        }
        this.dataList = new LinkedList<>();
    }

    public void insert(LinkedList<String> allNames, LinkedList curData, LinkedList<String> ignoreNames) throws NDException {
        LinkedList data = new LinkedList<>();
        int n = allNames.size();
        for (int i = 0; i < n; ++i) {
            if (ignoreNames.indexOf(allNames.get(i)) < 0)
                data.add(curData.get(i));
        }
        dataList.add(data);
    }

    public void insert(LinkedList<String> curNames, LinkedList curData, ArrayList<Table> tableList, LinkedList<String> ignoreNames) throws NDException {
        LinkedList data = new LinkedList();
        for (String colName: this.colNames) {
            Expression tmp = new Expression(1, colName);
            tmp.normalize(tableList, ignoreNames);
            String tableCol = tmp.getSymbol();
            int idx = curNames.indexOf(tableCol);
            if (idx < 0) {
                throw new NDException("Unknown column name: '" + colName + "'");
            }
            data.add(curData.get(idx));
        }
        insert(data);
    }

    public void insert(LinkedList data) {
        dataList.add(data);
    }

    public void show() {
        for (String colName: colNames) {
            System.out.printf("%15s ", colName);
        }
        System.out.println();
        for (LinkedList line: dataList) {
            for (Object o: line) {
                if (o == null) {
                    System.out.printf("           null ");
                } else
                    System.out.printf("%15s ", o.toString());
            }
            System.out.println();
        }
    }

    public String zipString() {
        StringBuffer buffer = new StringBuffer();
        int i = 0;
        int len = this.colNames.size();

        for (String name: this.colNames) {
            if (i == len - 1) buffer.append(name + "\n");
            else buffer.append(name + "|");
            i++;
        }

        for (LinkedList line: this.dataList) {
            i = 0;
            len = line.size();
            for (Object obj: line) {
                if (i == len - 1) {
                    if (obj == null) buffer.append("null\n");
                    else buffer.append(obj.toString() + "\n");
                }
                else {
                    if (obj == null) buffer.append("null|");
                    else buffer.append(obj.toString() + "|");
                }
                i++;
            }
        }
        return buffer.toString();
    }

    public LinkedList<String> getColNames() {
        return colNames;
    }

    public LinkedList<LinkedList> getDataList() {
        return dataList;
    }*/
}
