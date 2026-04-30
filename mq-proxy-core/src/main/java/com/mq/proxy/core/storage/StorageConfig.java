package com.mq.proxy.core.storage;

import java.util.HashMap;
import java.util.Map;

public class StorageConfig {
    private String adapterType;
    private String namesrvAddr;
    private String brokerAddr;
    private int connectTimeoutMillis = 3000;
    private Map<String, String> extraConfig = new HashMap<>();

    public String getAdapterType() {
        return adapterType;
    }

    public void setAdapterType(String adapterType) {
        this.adapterType = adapterType;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public Map<String, String> getExtraConfig() {
        return extraConfig;
    }

    public void setExtraConfig(Map<String, String> extraConfig) {
        this.extraConfig = extraConfig;
    }

    public String getExtraConfig(String key) {
        return extraConfig != null ? extraConfig.get(key) : null;
    }
}
