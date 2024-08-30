package cn.liubinbin.kdb.conf;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


/**
 * @author liubinbin
 * @date 2024/08/30
 */
public class ConfigTest {

    @Test
    public void shouldConfigReadRight() throws ConfigurationException, IOException {
        KdbConfig kdbConfig = new KdbConfig();
        int order = kdbConfig.getBtreeOrder();
        assertEquals(order, 3);
    }

}
