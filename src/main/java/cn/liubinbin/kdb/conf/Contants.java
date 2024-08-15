package cn.liubinbin.kdb.conf;

/**
 * @author liubinbin
 */
public class Contants {

    public static final String KDB_SERVER_PORT = "kdb.server.grpc.port";

    public static final int DEFAULT_KDB_SERVER_PORT = 50503;

    public static final String KDB_METRICS_PRINT = "kdb.metrics.print";

    public static final String dadf = "kdb.server.file.root.path";


    // --------------------------------------------------------------

    public static final String KDB_RMI_PORT = "kdb.rmi.port";

    // 132096 = 1024 * 129
    public static final int DEFAULT_CACHE_SLAB_SIZE = 132096;

    public static final int DEFAULT_CACHE_SLAB_MAX_COUNT = 128;

    public static final boolean DEFAULT_PAN_METRICS_PRINT = false;

    public static final String CACHE_TOTAL_SIZE = "pan.total.size";

    public static final String SLOT_SIZE = "pan.slot.size";

    public static final String HASH_MOD = "pan.hashmod";

    public static final String DEFAULT_SLOT_SIZE = "128,512,132096";

    public static final int DEFAULT_HASHMOD = 4;

    public static final int DEFAULT_KDB_RMI_PORT = 1099;

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
