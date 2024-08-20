package cn.liubinbin.kdb.conf;

import cn.liubinbin.kdb.server.table.TableType;
import cn.liubinbin.kdb.utils.Contants;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author liubinbin
 */
public class KdbConfig {

    private PropertiesConfiguration configuration = null;

    public KdbConfig() throws FileNotFoundException, ConfigurationException, IOException {
        configuration = new PropertiesConfiguration();
        configuration.read(new FileReader("conf/config.properties"));
    }

    public int getKdbServerPort() {
        return configuration.getInt(Contants.KDB_SERVER_PORT, Contants.DEFAULT_KDB_SERVER_PORT);
    }

    public String getTableRootPath() {
        return configuration.getString(Contants.KDB_SERVER_FILE_ROOT_PATH, Contants.DEFAULT_KDB_SERVER_FILE_ROOT_PATH);
    }

    public String getTableMetaFileName() {
        return configuration.getString(Contants.KDB_SERVER_META_FILE, Contants.DEFAULT_KDB_SERVER_META_FILE);
    }

    public TableType getTableType() {
        String kdbTableType = configuration.getString(Contants.KDB_SERVER_TABLE_TYPE, Contants.DEFAULT_KDB_SERVER_TABLE_TYPE);
        if (kdbTableType == null) {
            return TableType.Btree;
        }
        return TableType.getTableType(kdbTableType);
    }

    public String getMetaFullPath() {
        return getTableRootPath() + Contants.FILE_SEPARATOR + getTableMetaFileName();
    }

    public String getBackupFileExtension() {
        return configuration.getString(Contants.KDB_SERVER_BACKUP_FILE_EXTENSION, Contants.DEFAULT_KDB_SERVER_BACKUP_FILE_EXTENSION);
    }

    public String getDataFileExtension() {
        return configuration.getString(Contants.KDB_SERVER_DATA_FILE_EXTENSION, Contants.DEFAULT_KDB_SERVER_DATA_FILE_EXTENSION);
    }
}
