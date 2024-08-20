package cn.liubinbin.kdb.utils;

/**
 * @author liubinbin
 * @date 2024/08/14
 */
public class Contants {

    public static final String KDB_SERVER_PORT = "kdb.server.grpc.port";

    public static final int DEFAULT_KDB_SERVER_PORT = 50503;

    public static final String KDB_METRICS_PRINT = "kdb.metrics.print";

    public static final String KDB_SERVER_FILE_ROOT_PATH = "kdb.server.file.root.path";

    public static final String DEFAULT_KDB_SERVER_FILE_ROOT_PATH = "/tmp";

    public static final String KDB_SERVER_META_FILE = "kdb.server.meta.file";

    public static final String DEFAULT_KDB_SERVER_META_FILE = "meta.kdb";

    public static final String KDB_SERVER_TABLE_TYPE = "kdb.server.table.engine.type";

    public static final String DEFAULT_KDB_SERVER_TABLE_TYPE = "fake";

    public static final String KDB_SERVER_BACKUP_FILE_EXTENSION = "kdb.server.backup.file.extension";

    public static final String DEFAULT_KDB_SERVER_BACKUP_FILE_EXTENSION = ".bk";

    public static final String DEFAULT_KDB_DATABASE_NAME = "kdb";

    public static final String ROW_PRINT_SEPARATOR = "|";

    public static final String STATUS = "status";
    public static final String SUCCESS = "success";
    public static final String EXIT = "exit";


    public static final String FILE_SEPARATOR = "/";

    public static long MsInADay = 24 * 3600 * 1000;

    //  private int status;         // 4 byte, 0
    //  private long expireTime;    // 8 bytes, 0 + 4
    //  private int hash;           // 4 bytes, 0 + 4 + 8
    //  private int dataLen;        // 4 bytes, 0 + 4 + 8 + 4
    //  private int keyLength;      // 4 bytes, 0 + 4 + 8 + 4 + 4
    //  private int valueLength;    // 4 bytes, 0 + 4 + 8 + 4 + 4 + 4
    //  // data
    //  private byte[] key;         // key.length, 0 + 4 + 8 + 4 + 4 + 4 + 4
    //  private byte[] value;       // value.length, 0 + 4 + 8 + 4 + 4 + 4 + 4 + keyLength

    public static int STATUS_SHIFT = 0;
    public static int EXPIRETIME_SHIFT = 4;
    public static int HASH_SHIFT = 12;
    public static int DATALEN_SHIFT = 16;
    public static int KEYLENGTH_SHIFT = 20;
    public static int VALUELENGTH_SHIFT = 24;
    public static int KEYVALUE_SHIFT = 28;

    // linked version
    //     private int status;         // 4 byte, 0
    //     private long expireTime;    // 8 bytes, 0 + 4
    //     private int hash;           // 4 bytes, 0 + 4 + 8
    //     private int next;           // 4 bytes, 0 + 4 + 8 + 4
    //     private int dataLen;        // 4 bytes, 0 + 4 + 8 + 4 + 4
    //     private int keyLength;      // 4 bytes, 0 + 4 + 8 + 4 + 4 + 4
    //     private int valueLength;    // 4 bytes, 0 + 4 + 8 + 4 + 4 + 4 + 4
    //     // data
    //     private byte[] key;         // key.length, 0 + 4 + 8 + 4 + 4 + 4 + 4 + 4
    //     private byte[] value;       // value.length, 0 + 4 + 8 + 4 + 4 + 4 + 4 + 4 + keyLength
    public static int STATUS_SHIFT_LINKED = 0;
    public static int EXPIRETIME_SHIFT_LINKED = 4;
    public static int HASH_SHIFT_LINKED = 12;
    public static int NEXT_SHIFT_LINKED = 16;
    public static int DATALEN_SHIFT_LINKED = 20;
    public static int KEYLENGTH_SHIFT_LINKED = 24;
    public static int VALUELENGTH_SHIFT_LINKED = 28;
    public static int KEYVALUE_SHIFT_LINKED = 32;

}
