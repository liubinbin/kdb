package cn.liubinbin.kdb.conf;

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

    public int getRmiPort() {
        return configuration.getInt(Contants.KDB_METRICS_PRINT, Contants.DEFAULT_KDB_RMI_PORT);
    }

    public String getTableMetaPath() {
        return configuration.getString(Contants.KDB_SERVER_FILE_ROOT_PATH, Contants.KDB_SERVER_FILE_ROOT_PATH);
    }
}
