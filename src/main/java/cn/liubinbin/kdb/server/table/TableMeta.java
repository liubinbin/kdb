package cn.liubinbin.kdb.server.table;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

/**
 * 暂只支持 integer 和 varchar
 *  overview meta
 *    8个字节 table 个数
 *  table meta
 *    8个字节 tableName len
 *    8个字节 string
 *    8个字节 column 个数
 *      8个字节 columnName len
 *      8个字节 columnName
 */
public class TableMeta {

    String tableName;
    List<Column> columnList;

    public void writeTo(File file, int offset) {
        try (RandomAccessFile raf = new RandomAccessFile(new File("example.txt"), "rw")) {
            offset = 10; // 假设你想从第10个字节开始写入
            raf.seek(offset);
            raf.writeByte(65); // 写入字符 'A'
            System.out.println("Wrote byte at offset " + offset + ": A");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "TableMeta{" +
                "tableName='" + tableName + '\'' +
                ", columnList=" + columnList +
                '}';
    }
}
