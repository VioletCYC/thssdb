package cn.edu.thssdb.utils;

import cn.edu.thssdb.exception.InsertException;
import cn.edu.thssdb.type.ColumnType;
import com.sun.deploy.util.SyncAccess;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.LinkedList;
import java.util.List;

public class mNumber {
    /**
     * 把Object对象先转换成字符串然后转换成字节写入数组，Object是5种基本数据类型之一。
     * 当是String类型时，在前面填充6位整数，表示该字符串的长度，最大长度不超过999999（整数位数不足时前面补0）
     * 对于null的处理：字符串类型填充000000，其他类型不填充，只占长度
     * @param bytes 待写入内容的字符数组
     * @param pos 写入时的偏移量
     * @param value 待写入数组的Object
     * @param type value的数据类型
     * @return 写入的字节数
     */
    public static int toBytes(byte[] bytes, int pos, Object value, ColumnType type)
        throws InsertException, IOException {
        byte[] tmp;
        if (value == null) {
            if (type == ColumnType.STRING) {
                tmp = "000000".getBytes();
                System.arraycopy(tmp, 0, bytes, pos, tmp.length);
                return tmp.length;
            }
            else
                return byteOfType(type);
        }
        else {
            switch (type) {
                case INT:
                    tmp = Integer.toString((int) value).getBytes();
                    break;
                case LONG:
                    tmp = Long.toString((long) value).getBytes();
                    break;
                case FLOAT:
                    tmp = Float.toString((float) value).getBytes();
                    break;
                case DOUBLE:
                    tmp = Double.toString((double) value).getBytes();
                    break;
                case STRING:
                    //str_len是6位字节数组，保存字符串转成字节数组之后的长度
                    String length = String.valueOf(value.toString().getBytes().length);
                    byte[] str_len = new byte[6];
                    ByteArrayInputStream in = new ByteArrayInputStream(length.getBytes());
                    in.read(str_len);
                    System.out.println(str_len.length);

                    //把字符串长度str_len和字符串本身value拼接到tmp里
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    out.write(str_len);
                    out.write(value.toString().getBytes());
                    tmp = out.toByteArray();

//                String length = String.format("%06d", value.toString().length());
//                tmp = (length + value.toString()).getBytes();
                    break;
                default:
                    throw new InsertException(InsertException.TYPE_MATCH_ERROR);
            }
            System.arraycopy(tmp, 0, bytes, pos, tmp.length);

            return tmp.length;
        }
    }

    /**
     * 从字节数组中恢复一个Object类型的字段，Object是5种基本数据类型之一
     * @param list 空的记录存储空间
     * @param buffer 源字节数组
     * @param pos 偏移量，指定从哪里开始读
     * @param type 字段数据类型
     * @return 从字符数组中读了多少字节
     * @throws InsertException
     */
    public static int fromBytes(LinkedList list, byte[] buffer, int pos, ColumnType type)
            throws InsertException {
        int len = 0;
        byte[] tmp;
        switch (type) {
            case INT:
                len = Integer.BYTES;
                //从buffer中读出固定长度的字节
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                //转为整数
                list.add(Integer.parseInt(new String(tmp)));
                break;
            case LONG:
                len = Long.BYTES;
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                list.add(Long.parseLong(new String(tmp)));
                break;
            case FLOAT:
                len = Float.BYTES;
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                list.add(Float.parseFloat(new String(tmp)));
                break;
            case DOUBLE:
                len = Double.BYTES;
                tmp = new byte[len];
                System.arraycopy(buffer, pos, tmp, 0, len);
                list.add(Double.parseDouble(new String(tmp)));
                break;
            case STRING:
                byte[] tmp_strLength = new byte[6];
                System.arraycopy(buffer, pos, tmp_strLength, 0, 6);
                int strByteLength = Integer.parseInt(new String(tmp_strLength));
                if(strByteLength == 0) {
                    list.add(null);
                }
                else {
                    byte[] strBytes = new byte[strByteLength];
                    System.arraycopy(buffer, pos+6, strBytes, 0, strByteLength);
                    String str = new String(strBytes);
                    list.add(str);
                }
                len = tmp_strLength.length + strByteLength;
                break;
            default:
               break;
        }
        return len;
    }


    //获得5种基本数据类型的字节长度
    public static int byteOfType(ColumnType type) {
        int typeByte = 0;
        switch (type) {
            case INT:
                typeByte = Integer.BYTES;
                break;
            case LONG:
                typeByte = Long.BYTES;
                break;
            case FLOAT:
                typeByte = Float.BYTES;
                break;
            case DOUBLE:
                typeByte = Double.BYTES;
                break;
            case STRING:
                typeByte = Character.BYTES;
                break;
            default:
                break;
        }
        return typeByte;
    }

}
