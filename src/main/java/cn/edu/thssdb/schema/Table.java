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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;


//public class Table implements Iterable<Long> {
public class Table {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    //B+树节点的key是主键，value是文件索引数组，第一个值是起始位置，第二个值是长度
    public BPlusTree<Entry, ArrayList<Integer>> index;
    //一条记录的最大可能长度（字节）
    private int maxRowSize = 0;
    //指示下一条记录要写在文件中的起始位置
    public int lastBytePos = 0;
    private RandomAccessFile dataFile;
    private int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns)
            throws IOException, ClassNotFoundException {
        //TODO: 这几行之后应该挪到Manager里面

        // 如果文件存在：从文件中恢复B+树
        File treeFile = new File("data/" + databaseName + "/" + tableName + ".tree");
        if (treeFile.exists())
            index = deserialize_tree(tableName);
        else {
            index = new BPlusTree<Entry, ArrayList<Integer>>();
        }

        //如果数据文件不存在，则新建
        File f = new File("data/" + databaseName + "/" + tableName + ".data");
        if (!f.exists()) {
            f.createNewFile();
        }

        dataFile = new RandomAccessFile("data/" + databaseName + "/" + tableName + ".data", "rw");

        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<Column>();
        Collections.addAll(this.columns, columns);

        //计算记录的最大长度
        maxRowSize = 0;
        for (Column col : columns) {
            maxRowSize = maxRowSize + mNumber.byteOfType(col.getType());
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

        //把values转换成Row
        Entry key = getKey(values);
        Row newRow = new Row();
        for (int i = 0; i < columns.size(); i++) {
            newRow.appendOneEntry(Entry.convertType(values.get(i), columns.get(i).getType()));
        }

        //将该条记录持久化存储，获得文件的存储位置
        ArrayList<Integer> storePos = serialize_row(newRow);

        //在B+树中插入新的节点
        index.put(key, storePos);
    }


    public void delete(LinkedList values) {
        //TODO: 合法性检查（是不是在查询时已经保证了？）
        Entry key = getKey(values);
        index.remove(key);
    }

    public void update(LinkedList old_values, LinkedList new_values)
            throws InsertException, IOException {
        delete(old_values);
        insert(new_values);
    }

    private ArrayList<Integer> serialize_row(Row row)
            throws IOException {
        byte[] rowData = new byte[maxRowSize];
        Arrays.fill(rowData, (byte) 0);

        int recordSize = 0; //该条记录所占的字节数
        int n = columns.size();
        for (int i = 0; i < n; ++i) {
            recordSize += mNumber.toBytes(rowData, recordSize, row.get(i), columns.get(i).getType());
        }

        this.dataFile.seek(lastBytePos);
        this.dataFile.write(rowData, 0, recordSize);

        //这条记录存储的位置
        ArrayList<Integer> storePos = new ArrayList<>(2);
        storePos.add(lastBytePos);
        storePos.add(recordSize);

        //文件末尾指针向前移动一次
        lastBytePos += recordSize;

        return storePos;
    }

    public void serialize_tree()
            throws IOException {
        //先检查文件是否存在
        File treeFile = new File("data/" + databaseName + "/" + tableName + ".tree");
        if (!treeFile.exists()) {
            treeFile.createNewFile();
        }

        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("data/" + databaseName + "/" + tableName + ".tree"));
        oos.writeObject(databaseName);
        oos.writeObject(tableName);
        oos.writeObject(columns);
        oos.writeObject((Integer)lastBytePos);
        oos.writeObject(index);
        oos.close();
    }

    public BPlusTree<Entry, ArrayList<Integer>> deserialize_tree(String tableName)
            throws IOException, ClassNotFoundException {
        //创建一个ObjectInputStream输入流
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("data/" + databaseName + "/" + tableName + ".tree"));
        databaseName = (String) ois.readObject();
        tableName = (String) ois.readObject();
        columns = (ArrayList<Column>) ois.readObject();
        lastBytePos = (Integer) ois.readObject();
        index = (BPlusTree<Entry, ArrayList<Integer>>) ois.readObject();
        ois.close();
        return index;
    }

    //按主键从文件中获取一条记录
    public Row getRowFromFile(Entry key)
            throws SearchException, IOException {

        ArrayList<Integer> storePos = index.get(key);
        int offset = storePos.get(0);   //文件起始位置
        int length = storePos.get(1);   //读入的字节数（记录长度）
        byte[] buffer = new byte[length];
        dataFile.seek(offset);
        dataFile.read(buffer, 0, length);

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

//    private class TableIterator implements Iterator<Integer> {
//        private Iterator<Pair<Entry, ArrayList<Integer>>> iterator;
//
//        TableIterator(Table table) {
//            this.iterator = table.index.iterator();
//        }
//
//        @Override
//        public boolean hasNext() {
//            return iterator.hasNext();
//        }
//
//        @Override
//        public ArrayList<Integer> next() {
//            return iterator.next().getValue();
//        }
//    }
//
//    @Override
//    public Iterator<ArrayList<Integer>> iterator() {
//        return new TableIterator(this);
//    }

    public void close()
            throws IOException {
        dataFile.close();
        serialize_tree();
    }

    public ArrayList<Column> getColumns(){
        return columns;
    }

}