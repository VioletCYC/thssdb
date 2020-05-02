package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.mNumber;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public ColumnType getType() {
    return type;
  }

  //获得以字节为单位的最大长度
  public int getMaxByteLength() {
    int typeByte = mNumber.byteOfType(type);
    return maxLength * typeByte;
  }

  public boolean isPrimary() {
    return primary != 0;
  }

  public boolean canBeNull() {
    return !notNull;
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }
}
