package com.mq.proxy.core.config;

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
        if (props.containsKey("proxy.pullExecutorThreadNums")) {
            config.setPullExecutorThreadNums(parseInt("proxy.pullExecutorThreadNums",
                    props.getProperty("proxy.pullExecutorThreadNums")));
        }
        if (props.containsKey("proxy.routeCacheExpireMillis")) {
            config.setRouteCacheExpireMillis(parseLong("proxy.routeCacheExpireMillis", props.getProperty("proxy.routeCacheExpireMillis")));
        }

        if (props.containsKey("proxy.tlsEnabled")) {
            config.setTlsEnabled(Boolean.parseBoolean(props.getProperty("proxy.tlsEnabled")));
        }
        if (props.containsKey("proxy.tlsCertPath")) {
            config.setTlsCertPath(props.getProperty("proxy.tlsCertPath"));
        }
        if (props.containsKey("proxy.tlsKeyPath")) {
            config.setTlsKeyPath(props.getProperty("proxy.tlsKeyPath"));
        }
        if (props.containsKey("proxy.tlsTrustCertPath")) {
            config.setTlsTrustCertPath(props.getProperty("proxy.tlsTrustCertPath"));
        }
        if (props.containsKey("proxy.tlsClientAuth")) {
            config.setTlsClientAuth(Boolean.parseBoolean(props.getProperty("proxy.tlsClientAuth")));
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

    public static ProxyConfig loadDefault() {
        return new ProxyConfig();
    }
}
