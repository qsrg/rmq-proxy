package com.mq.proxy.core.config;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ProxyConfigLoaderTest {

    @Test
    public void shouldNotExposeBrokerAddrField() {
        boolean hasBrokerAddrField = Arrays.stream(ProxyConfig.class.getDeclaredFields())
                .map(Field::getName)
                .anyMatch("brokerAddr"::equals);

        assertFalse("brokerAddr field should be removed from ProxyConfig", hasBrokerAddrField);
    }

    @Test
    public void shouldLoadNamesrvAddrFromProperties() {
        Properties props = new Properties();
        props.setProperty("proxy.namesrvAddr", "127.0.0.1:9876");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertEquals("127.0.0.1:9876", config.getNamesrvAddr());
    }
}
