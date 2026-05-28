package com.mq.proxy.core.config;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;
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
            config.setListenPort(Integer.parseInt(props.getProperty("proxy.listenPort")));
        }
        if (props.containsKey("proxy.host")) {
            config.setProxyHost(props.getProperty("proxy.host"));
        }
        if (props.containsKey("proxy.namesrvAddr")) {
            config.setNamesrvAddr(props.getProperty("proxy.namesrvAddr"));
        }
        if (props.containsKey("proxy.storageAdapterType")) {
            config.setStorageAdapterType(props.getProperty("proxy.storageAdapterType"));
        }
        if (props.containsKey("proxy.brokerAddr")) {
            config.setBrokerAddr(props.getProperty("proxy.brokerAddr"));
        }
        if (props.containsKey("proxy.connectTimeoutMillis")) {
            config.setConnectTimeoutMillis(Integer.parseInt(props.getProperty("proxy.connectTimeoutMillis")));
        }
        if (props.containsKey("proxy.registerProxyToNameServer")) {
            config.setRegisterProxyToNameServer(Boolean.parseBoolean(props.getProperty("proxy.registerProxyToNameServer")));
        }
        if (props.containsKey("proxy.proxyBrokerName")) {
            config.setProxyBrokerName(props.getProperty("proxy.proxyBrokerName"));
        }
        if (props.containsKey("proxy.proxyClusterName")) {
            config.setProxyClusterName(props.getProperty("proxy.proxyClusterName"));
        }
        if (props.containsKey("proxy.bossThreadNums")) {
            config.setBossThreadNums(Integer.parseInt(props.getProperty("proxy.bossThreadNums")));
        }
        if (props.containsKey("proxy.workerThreadNums")) {
            config.setWorkerThreadNums(Integer.parseInt(props.getProperty("proxy.workerThreadNums")));
        }
        if (props.containsKey("proxy.routeCacheExpireMillis")) {
            config.setRouteCacheExpireMillis(Long.parseLong(props.getProperty("proxy.routeCacheExpireMillis")));
        }

        // TLS配置
        if (props.containsKey("proxy.tlsEnabled")) {
            config.setTlsEnabled(Boolean.parseBoolean(props.getProperty("proxy.tlsEnabled")));
        }
        if (props.containsKey("proxy.tlsKeyStorePath")) {
            config.setTlsKeyStorePath(props.getProperty("proxy.tlsKeyStorePath"));
        }
        if (props.containsKey("proxy.tlsKeyStorePassword")) {
            config.setTlsKeyStorePassword(props.getProperty("proxy.tlsKeyStorePassword"));
        }
        if (props.containsKey("proxy.tlsTrustStorePath")) {
            config.setTlsTrustStorePath(props.getProperty("proxy.tlsTrustStorePath"));
        }
        if (props.containsKey("proxy.tlsTrustStorePassword")) {
            config.setTlsTrustStorePassword(props.getProperty("proxy.tlsTrustStorePassword"));
        }
        if (props.containsKey("proxy.tlsKeyStoreType")) {
            config.setTlsKeyStoreType(props.getProperty("proxy.tlsKeyStoreType"));
        }
        if (props.containsKey("proxy.tlsClientAuth")) {
            config.setTlsClientAuth(Boolean.parseBoolean(props.getProperty("proxy.tlsClientAuth")));
        }

        Map<String, String> topicRouteConfig = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("topic.route.")) {
                String topic = key.substring("topic.route.".length());
                topicRouteConfig.put(topic, props.getProperty(key));
            }
        }
        config.setTopicRouteConfig(topicRouteConfig);

        return config;
    }

    public static ProxyConfig loadDefault() {
        return new ProxyConfig();
    }
}
