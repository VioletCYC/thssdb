package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Entry, Row> index;
    private int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns) {
        // TODO
        File file = new File(tableName + ".tree");
        // 如果文件存在：从文件中恢复B+树
        if (file.exists())
            recover();

        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<Column>();
        Collections.addAll(this.columns, columns);
    }

    private void recover() {
        // TODO
    }

    public void insert(LinkedList values) {
        // TODO
        if (values == null)
            return ;
        //合法性检查
        try {
            legalCheck(values);
        } catch (InsertException e) {
            e.printStackTrace();
            return;
        }

        //在B+树中查找到相应的索引
        //把values转换成Row
        Entry key = getKey(values);
        Row newRow = new Row();
        for (int i = 0; i < columns.size(); i++) {
            newRow.appendOneEntry(Entry.convertType(values.get(i), columns.get(i).getType()));
        }
        index.put(key, newRow);

        // TODO: 按索引指示的文件位置，将该条记录持久化存储

    }

    public void delete(LinkedList values) {
        // TODO
        //TODO: 合法性检查（是不是在查询时已经保证了？）
        Entry key = getKey(values);
        index.remove(key);
    }

    public void update(LinkedList old_values, LinkedList new_values) {
        // TODO

        delete(old_values);
        insert(new_values);
    }

    private void serialize() {
        // TODO
    }

    private ArrayList<Row> deserialize() {
        // TODO
        return null;
    }

    //字段合法性检查
    private void legalCheck(LinkedList values) {
        // 列数
        if (values.size() != columns.size())
            throw new InsertException(InsertException.COLUMN_LENGTH_MATCH_ERROR);

        // 各列元素类型
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            boolean typeError = false;
            switch (columns.get(i).getType()) {
                case INT:
                    if (!(value instanceof Integer))
                        typeError = true;
                    break;
                case LONG:
                    if (!(value instanceof Long))
                        typeError = true;
                    break;
                case FLOAT:
                    if (!(value instanceof Float))
                        typeError = true;
                    break;
                case DOUBLE:
                    if (!(value instanceof Double))
                        typeError = true;
                    break;
                case STRING:
                    if (!(value instanceof String))
                        typeError = true;
                    break;
                default:
                    break;
            }
            if (typeError)
                throw new InsertException(InsertException.TYPE_MATCH_ERROR);
        }

        // 主键重复
        Entry key = getKey(values);
        if (index.contains(key)) {
            throw new InsertException(InsertException.KEY_DUPLICATE_ERROR);
        }
    }

    //获得待操作数据的主键
    private Entry getKey(LinkedList values) {
        Entry key = null;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isPrimary()) {
                key = Entry.convertType(values.get(i), columns.get(i).getType());
            }
        }
        return key;
    }

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Entry, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().getValue();
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }
}
