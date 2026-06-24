package com.mq.proxy.core.config;

import com.mq.proxy.core.server.TlsMode;

import java.io.FileInputStream;
import java.util.Properties;

public class ProxyConfigLoader {

    public static ProxyConfig loadFromFile(String filePath) {
        try {
            Properties props = new Properties();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                props.load(fis);
            }
            return loadFromProperties(props);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load config from file: " + filePath, e);
        }
    }

    public static ProxyConfig loadFromProperties(Properties props) {
        ProxyConfig config = new ProxyConfig();

        if (props.containsKey("proxy.listenPort")) {
            config.setListenPort(parseInt("proxy.listenPort", props.getProperty("proxy.listenPort")));
        }
        if (props.containsKey("proxy.host")) {
            config.setProxyHost(props.getProperty("proxy.host"));
        }
        if (props.containsKey("proxy.namesrvAddr")) {
            config.setNamesrvAddr(props.getProperty("proxy.namesrvAddr"));
        }
        if (props.containsKey("proxy.connectTimeoutMillis")) {
            config.setConnectTimeoutMillis(parseInt("proxy.connectTimeoutMillis", props.getProperty("proxy.connectTimeoutMillis")));
        }
        if (props.containsKey("proxy.bossThreadNums")) {
            config.setBossThreadNums(parseInt("proxy.bossThreadNums", props.getProperty("proxy.bossThreadNums")));
        }
        if (props.containsKey("proxy.workerThreadNums")) {
            config.setWorkerThreadNums(parseInt("proxy.workerThreadNums", props.getProperty("proxy.workerThreadNums")));
        }
        if (props.containsKey("proxy.requestProcessorThreadNums")) {
            config.setRequestProcessorThreadNums(parseInt("proxy.requestProcessorThreadNums",
                    props.getProperty("proxy.requestProcessorThreadNums")));
        }
        if (props.containsKey("proxy.pullExecutorThreadNums")) {
            config.setPullExecutorThreadNums(parseInt("proxy.pullExecutorThreadNums",
                    props.getProperty("proxy.pullExecutorThreadNums")));
        }
        if (props.containsKey("proxy.routeCacheExpireMillis")) {
            config.setRouteCacheExpireMillis(parseLong("proxy.routeCacheExpireMillis", props.getProperty("proxy.routeCacheExpireMillis")));
        }
        if (props.containsKey("proxy.upstreamClientAsyncSemaphoreValue")) {
            config.setUpstreamClientAsyncSemaphoreValue(parseInt("proxy.upstreamClientAsyncSemaphoreValue",
                    props.getProperty("proxy.upstreamClientAsyncSemaphoreValue")));
        }
        if (props.containsKey("proxy.upstreamProducerAsyncSemaphoreValue")) {
            config.setUpstreamProducerAsyncSemaphoreValue(parseInt("proxy.upstreamProducerAsyncSemaphoreValue",
                    props.getProperty("proxy.upstreamProducerAsyncSemaphoreValue")));
        }
        if (props.containsKey("proxy.upstreamPullAsyncSemaphoreValue")) {
            config.setUpstreamPullAsyncSemaphoreValue(parseInt("proxy.upstreamPullAsyncSemaphoreValue",
                    props.getProperty("proxy.upstreamPullAsyncSemaphoreValue")));
        }
        if (props.containsKey("proxy.upstreamClientChannelPoolSize")) {
            config.setUpstreamClientChannelPoolSize(parseInt("proxy.upstreamClientChannelPoolSize",
                    props.getProperty("proxy.upstreamClientChannelPoolSize")));
        }
        if (props.containsKey("proxy.upstreamClientKeepAliveIntervalSeconds")) {
            config.setUpstreamClientKeepAliveIntervalSeconds(parseInt("proxy.upstreamClientKeepAliveIntervalSeconds",
                    props.getProperty("proxy.upstreamClientKeepAliveIntervalSeconds")));
        }

        String downstreamTlsEnabled = getProperty(props, "proxy.downstream.tls.enabled", "proxy.tlsEnabled");
        if (downstreamTlsEnabled != null) {
            config.setTlsEnabled(Boolean.parseBoolean(downstreamTlsEnabled));
        }
        String downstreamTlsCertPath = getProperty(props, "proxy.downstream.tls.certPath", "proxy.tlsCertPath");
        if (downstreamTlsCertPath != null) {
            config.setTlsCertPath(downstreamTlsCertPath);
        }
        String downstreamTlsKeyPath = getProperty(props, "proxy.downstream.tls.keyPath", "proxy.tlsKeyPath");
        if (downstreamTlsKeyPath != null) {
            config.setTlsKeyPath(downstreamTlsKeyPath);
        }
        String downstreamTlsTrustCertPath = getProperty(props, "proxy.downstream.tls.trustCertPath", "proxy.tlsTrustCertPath");
        if (downstreamTlsTrustCertPath != null) {
            config.setTlsTrustCertPath(downstreamTlsTrustCertPath);
        }
        String downstreamTlsClientAuth = getProperty(props, "proxy.downstream.tls.clientAuth", "proxy.tlsClientAuth");
        if (downstreamTlsClientAuth != null) {
            config.setTlsClientAuth(Boolean.parseBoolean(downstreamTlsClientAuth));
        }
        String downstreamTlsMode = getProperty(props, "proxy.downstream.tls.mode", "proxy.tlsMode");
        if (downstreamTlsMode != null) {
            config.setTlsMode(TlsMode.parse(downstreamTlsMode));
        }

        String upstreamBrokerTlsEnabled = getProperty(props, "proxy.upstream.broker.tls.enabled", "proxy.upstreamTlsEnabled");
        if (upstreamBrokerTlsEnabled != null) {
            config.setUpstreamTlsEnabled(Boolean.parseBoolean(upstreamBrokerTlsEnabled));
        }
        String upstreamBrokerTlsClientCertPath = getProperty(props,
                "proxy.upstream.broker.tls.clientCertPath", "proxy.upstreamTlsClientCertPath");
        if (upstreamBrokerTlsClientCertPath != null) {
            config.setUpstreamTlsClientCertPath(upstreamBrokerTlsClientCertPath);
        }
        String upstreamBrokerTlsClientKeyPath = getProperty(props,
                "proxy.upstream.broker.tls.clientKeyPath", "proxy.upstreamTlsClientKeyPath");
        if (upstreamBrokerTlsClientKeyPath != null) {
            config.setUpstreamTlsClientKeyPath(upstreamBrokerTlsClientKeyPath);
        }

        return config;
    }

    private static int parseInt(String key, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value for config '" + key + "': " + value, e);
        }
    }

    private static long parseLong(String key, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long value for config '" + key + "': " + value, e);
        }
    }

    private static String getProperty(Properties props, String canonicalKey, String legacyKey) {
        if (props.containsKey(canonicalKey)) {
            return props.getProperty(canonicalKey);
        }
        if (legacyKey != null && props.containsKey(legacyKey)) {
            return props.getProperty(legacyKey);
        }
        return null;
    }

    public static ProxyConfig loadDefault() {
        return new ProxyConfig();
    }
}
