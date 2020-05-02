package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.exception.SearchException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.mNumber;
import javafx.util.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Long> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    //B+树节点的key是主键，value是文件索引
    public BPlusTree<Entry, Long> index;
    //一条记录的最大长度（字节）
    private int rowSize;
    //指示下一条记录要写在文件中的位置
    private long rowNum = 0;
    private RandomAccessFile dataFile;
    private int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns)
            throws IOException {

        // 如果文件存在：从文件中恢复B+树
        File treeFile = new File("data/" + tableName + ".tree");
        if (treeFile.exists())
            recover();
        else {
            index = new BPlusTree<Entry, Long>();
        }

        //如果数据文件不存在，则新建
        File f = new File("data/" + tableName + ".data");
        if (!f.exists()) {
            f.createNewFile();
        }

        dataFile = new RandomAccessFile("data/" + tableName + ".data", "rw");

        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<Column>();
        Collections.addAll(this.columns, columns);

        //计算记录的最大长度
        rowSize = 0;
        for (Column col : columns) {
            rowSize = rowSize + col.getMaxByteLength() + 6; //6是存储字符串长度的6个字节
        }
    }

    private void recover() {
        // TODO
    }

    public void insert(LinkedList values)
            throws InsertException, IOException {
        // TODO
        if (values == null)
            return;
        //合法性检查
        legalCheck(values);

        //在B+树中查找到相应的索引
        //把values转换成Row
        Entry key = getKey(values);
        Row newRow = new Row();
        for (int i = 0; i < columns.size(); i++) {
            newRow.appendOneEntry(Entry.convertType(values.get(i), columns.get(i).getType()));
        }
        index.put(key, rowNum);

        //按索引指示的文件位置，将该条记录持久化存储
        serialize_row(newRow);
    }

    public void delete(LinkedList values) {
        // TODO
        //TODO: 合法性检查（是不是在查询时已经保证了？）
        Entry key = getKey(values);
        index.remove(key);
    }

    public void update(LinkedList old_values, LinkedList new_values)
            throws InsertException, IOException {
        // TODO

        delete(old_values);
        insert(new_values);
    }

    private void serialize_row(Row row) throws IOException {
        // TODO
        byte[] rowData = new byte[rowSize];
        Arrays.fill(rowData, (byte) 0);
        int pos = 0;
        int n = columns.size();
        for (int i = 0; i < n; ++i) {
//            if(row.get(i) == null) {
//                pos += mNumber.byteOfType(columns.get(i).)
//            }
            pos += mNumber.toBytes(rowData, pos, row.get(i), columns.get(i).getType());
        }

        this.dataFile.seek(rowNum * rowSize);
        this.dataFile.write(rowData, 0, rowSize);
        rowNum++;

    }

    public void serialize_tree() {

    }

    private ArrayList<Row> deserialize() {
        // TODO

        return null;
    }

    //从数据文件的指定行中获取一条记录
    public Row getRowFromFile(Long rowNum)
            throws SearchException, IOException {
        if (rowNum < 0 || rowNum >= this.rowNum) {
            throw new SearchException(SearchException.ROW_NUM_ERROR);
        }

        long offset = rowSize * rowNum;
        byte[] buffer = new byte[this.rowSize];
        dataFile.seek(offset);
        dataFile.read(buffer, 0, rowSize);

        LinkedList row = new LinkedList();

        int pos = 0;
        for (int i = 0; i < columns.size(); ++i) {
            pos += mNumber.fromBytes(row, buffer, pos, columns.get(i).getType());
        }
        //把row转换成Row类型
        Row newRow = new Row();
        for (int i = 0; i < columns.size(); i++) {
            newRow.appendOneEntry(Entry.convertType(row.get(i), columns.get(i).getType()));
        }

        return newRow;
    }

    //字段合法性检查
    private void legalCheck(LinkedList values) {
        // 列数
        if (values.size() != columns.size())
            throw new InsertException(InsertException.COLUMN_LENGTH_MATCH_ERROR);

        // 各列元素类型
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if(value == null) {
                if(columns.get(i).canBeNull())
                    continue;
            }
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
                break;
            }
        }
        return key;
    }

    private class TableIterator implements Iterator<Long> {
        private Iterator<Pair<Entry, Long>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Long next() {
            return iterator.next().getValue();
        }
    }

    @Override
    public Iterator<Long> iterator() {
        return new TableIterator(this);
    }
}
