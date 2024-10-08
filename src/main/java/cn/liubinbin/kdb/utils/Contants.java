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

    public static final String KDB_SERVER_DATA_FILE_EXTENSION = "kdb.server.data.file.extension";

    public static final String KDB_SERVER_TABLE_ENGINE_BTREE_ORDER = "kdb.server.table.engine.btree.order";

    public static final int DEFAULT_KDB_SERVER_TABLE_ENGINE_BTREE_ORDER = 3;

    public static final String DEFAULT_KDB_SERVER_DATA_FILE_EXTENSION = ".kdt";

    public static final String DEFAULT_KDB_SERVER_BACKUP_FILE_EXTENSION = ".bk";

    public static final String DEFAULT_KDB_DATABASE_NAME = "kdb";

    public static final String ROW_PRINT_SEPARATOR = "|";

    public static final String STATUS = "status";
    public static final String SUCCESS = "success";
    public static final String TABLE_NOT_EXIST = "table not exist";
    public static final String EXIT = "exit";

    public static final String FILE_SEPARATOR = "/";

    public static int ROOT_BIT_SHIFT = 0;
    public static int LEAF_BIT_SHIFT = 1;
    public static int INTEGER_SHIFT = 4;

    public static int ROOT_NODE_ID = 0;

    // nodeId
    // status  xxxxxxIsLeafIsRoot
    // rowCount
    // ChildrenCount
    // Children.Nodeid
    // ChildrenSep
    public static int NODE_ID_SHIFT = 0;
    public static int STATUS_SHIFT = 4;
    public static int ROW_COUNT = 8;
    public static int NEXT_NODE_ID_SHIFT = 12;
    public static int CHILDREN_COUNT_SHIFT = 16;
    // 1024
    public static int META_SHIFT = 1024;

    public static int NULL_NEXT_NODE_ID = -1;

    public static int DEFAULT_LIMIT = 10;

    public static String COUNT_START = "COUNT(*)";

    public static int DEFAULT_COUNT_STAR = -1;

}
