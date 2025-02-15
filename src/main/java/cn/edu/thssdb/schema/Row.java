package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    protected ArrayList<Entry> entries;

    public Row() {
        this.entries = new ArrayList<>();
    }

    public Row(Entry[] entries) {
        this.entries = new ArrayList<>(Arrays.asList(entries));
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public void appendEntries(ArrayList<Entry> entries) {
        this.entries.addAll(entries);
    }

    public void appendOneEntry(Entry entry) {
        this.entries.add(entry);
    }

    //获得第i个数据（Object）
    public Object get(int i) {
        if(entries.get(i) == null)
            return null;
        else
            return entries.get(i).value;
    }

    public String toString() {
        if (entries == null)
            return "EMPTY";
        StringJoiner sj = new StringJoiner(", ");
        for (Entry e : entries) {
            if(e == null)
                sj.add("");
            else
                sj.add(e.toString());
        }
        return sj.toString();
    }
}
