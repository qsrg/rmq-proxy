package com.mq.proxy.core.storage;

public class StorageConfig {
    private String namesrvAddr;
    private int connectTimeoutMillis = 3000;
    private int upstreamClientAsyncSemaphoreValue = 4096;
    private int upstreamClientChannelPoolSize = 4;
    private int upstreamClientKeepAliveIntervalSeconds = 30;

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getUpstreamClientAsyncSemaphoreValue() {
        return upstreamClientAsyncSemaphoreValue;
    }

    public void setUpstreamClientAsyncSemaphoreValue(int upstreamClientAsyncSemaphoreValue) {
        this.upstreamClientAsyncSemaphoreValue = upstreamClientAsyncSemaphoreValue;
    }

    public int getUpstreamClientChannelPoolSize() {
        return upstreamClientChannelPoolSize;
    }

    public void setUpstreamClientChannelPoolSize(int upstreamClientChannelPoolSize) {
        this.upstreamClientChannelPoolSize = upstreamClientChannelPoolSize;
    }

    public int getUpstreamClientKeepAliveIntervalSeconds() {
        return upstreamClientKeepAliveIntervalSeconds;
    }

    public void setUpstreamClientKeepAliveIntervalSeconds(int upstreamClientKeepAliveIntervalSeconds) {
        this.upstreamClientKeepAliveIntervalSeconds = upstreamClientKeepAliveIntervalSeconds;
    }
}
