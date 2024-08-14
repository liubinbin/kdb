package cn.liubinbin.kdb.conf;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author liubinbin
 */
public class Config {

    private PropertiesConfiguration configuration = null;

    public Config() throws FileNotFoundException, ConfigurationException, IOException {
        configuration = new PropertiesConfiguration();
        configuration.read(new FileReader("conf/config.properties"));
    }

    public int getKdbServerPort() {
        return configuration.getInt(Contants.KDB_SERVER_PORT, Contants.DEFAULT_KDB_SERVER_PORT);
    }

    public int getNettyThreadCount() {
        return configuration.getInt(Contants.PAN_NETTY_SERVER_THREAD_COUNT,
                Contants.DEFAULT_CACHE_NETTY_SERVER_THREAD_COUNT);
    }

    public int getRmiPort() {
        return configuration.getInt(Contants.PAN_METRICS_PRINT, Contants.DEFAULT_PAN_RMI_PORT);
    }
}
