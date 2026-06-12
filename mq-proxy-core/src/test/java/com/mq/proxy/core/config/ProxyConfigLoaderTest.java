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

    @Test
    public void shouldLoadPullExecutorThreadNumsFromProperties() {
        Properties props = new Properties();
        props.setProperty("proxy.pullExecutorThreadNums", "96");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertEquals(96, config.getPullExecutorThreadNums());
    }

    @Test
    public void shouldLoadUpstreamClientLimitsFromProperties() {
        Properties props = new Properties();
        props.setProperty("proxy.upstreamClientAsyncSemaphoreValue", "512");
        props.setProperty("proxy.upstreamClientChannelPoolSize", "4");
        props.setProperty("proxy.upstreamClientKeepAliveIntervalSeconds", "30");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertEquals(512, config.getUpstreamClientAsyncSemaphoreValue());
        assertEquals(4, config.getUpstreamClientChannelPoolSize());
        assertEquals(30, config.getUpstreamClientKeepAliveIntervalSeconds());
    }
}
