package com.mq.proxy.core.config;

import com.mq.proxy.core.server.TlsMode;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public void shouldLoadRequestProcessorThreadNumsFromProperties() {
        Properties props = new Properties();
        props.setProperty("proxy.requestProcessorThreadNums", "64");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertEquals(64, config.getRequestProcessorThreadNums());
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

    @Test
    public void shouldLoadDownstreamTlsConfigFromCanonicalProperties() {
        Properties props = new Properties();
        props.setProperty("proxy.downstream.tls.enabled", "true");
        props.setProperty("proxy.downstream.tls.mode", "enforcing");
        props.setProperty("proxy.downstream.tls.certPath", "/path/to/server.crt");
        props.setProperty("proxy.downstream.tls.keyPath", "/path/to/server.key");
        props.setProperty("proxy.downstream.tls.trustCertPath", "/path/to/ca.crt");
        props.setProperty("proxy.downstream.tls.clientAuth", "true");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertTrue(config.isTlsEnabled());
        assertEquals(TlsMode.ENFORCING, config.getTlsMode());
        assertEquals("/path/to/server.crt", config.getTlsCertPath());
        assertEquals("/path/to/server.key", config.getTlsKeyPath());
        assertEquals("/path/to/ca.crt", config.getTlsTrustCertPath());
        assertTrue(config.isTlsClientAuth());
    }

    @Test
    public void shouldDefaultTlsModeToPermissive() {
        Properties props = new Properties();

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertEquals(TlsMode.PERMISSIVE, config.getTlsMode());
        assertFalse(config.isTlsEnabled());
        assertFalse(config.isUpstreamTlsEnabled());
    }

    @Test
    public void shouldLoadUpstreamBrokerTlsConfigFromCanonicalProperties() {
        Properties props = new Properties();
        props.setProperty("proxy.upstream.broker.tls.enabled", "true");
        props.setProperty("proxy.upstream.broker.tls.clientCertPath", "/path/to/proxy-client.crt");
        props.setProperty("proxy.upstream.broker.tls.clientKeyPath", "/path/to/proxy-client.key");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertTrue(config.isUpstreamTlsEnabled());
        assertEquals("/path/to/proxy-client.crt", config.getUpstreamTlsClientCertPath());
        assertEquals("/path/to/proxy-client.key", config.getUpstreamTlsClientKeyPath());
    }

    @Test
    public void shouldLoadLegacyTlsAliasesForCompatibility() {
        Properties props = new Properties();
        props.setProperty("proxy.tlsEnabled", "true");
        props.setProperty("proxy.tlsMode", "enforcing");
        props.setProperty("proxy.tlsCertPath", "/path/to/legacy-server.crt");
        props.setProperty("proxy.tlsKeyPath", "/path/to/legacy-server.key");
        props.setProperty("proxy.tlsClientAuth", "true");
        props.setProperty("proxy.upstreamTlsEnabled", "true");
        props.setProperty("proxy.upstreamTlsClientCertPath", "/path/to/legacy-client.crt");
        props.setProperty("proxy.upstreamTlsClientKeyPath", "/path/to/legacy-client.key");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertTrue(config.isTlsEnabled());
        assertEquals(TlsMode.ENFORCING, config.getTlsMode());
        assertEquals("/path/to/legacy-server.crt", config.getTlsCertPath());
        assertEquals("/path/to/legacy-server.key", config.getTlsKeyPath());
        assertTrue(config.isTlsClientAuth());
        assertTrue(config.isUpstreamTlsEnabled());
        assertEquals("/path/to/legacy-client.crt", config.getUpstreamTlsClientCertPath());
        assertEquals("/path/to/legacy-client.key", config.getUpstreamTlsClientKeyPath());
    }

    @Test
    public void shouldPreferCanonicalTlsPropertiesOverLegacyAliases() {
        Properties props = new Properties();
        props.setProperty("proxy.tlsEnabled", "false");
        props.setProperty("proxy.downstream.tls.enabled", "true");
        props.setProperty("proxy.tlsMode", "disabled");
        props.setProperty("proxy.downstream.tls.mode", "enforcing");
        props.setProperty("proxy.upstreamTlsEnabled", "false");
        props.setProperty("proxy.upstream.broker.tls.enabled", "true");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertTrue(config.isTlsEnabled());
        assertEquals(TlsMode.ENFORCING, config.getTlsMode());
        assertTrue(config.isUpstreamTlsEnabled());
    }

    @Test
    public void shouldKeepUpstreamAndDownstreamTlsIndependent() {
        // 下游 TLS 关闭，上游 TLS 开启（broker enforcing 模式场景）
        Properties props = new Properties();
        props.setProperty("proxy.downstream.tls.enabled", "false");
        props.setProperty("proxy.upstream.broker.tls.enabled", "true");

        ProxyConfig config = ProxyConfigLoader.loadFromProperties(props);

        assertFalse("downstream TLS should be disabled", config.isTlsEnabled());
        assertTrue("upstream TLS should be enabled", config.isUpstreamTlsEnabled());
    }
}
