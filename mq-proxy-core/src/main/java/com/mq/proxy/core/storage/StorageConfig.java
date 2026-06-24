package com.mq.proxy.core.storage;

public class StorageConfig {
    private String namesrvAddr;
    private int connectTimeoutMillis = 3000;
    private int upstreamClientAsyncSemaphoreValue = 4096;
    private int upstreamProducerAsyncSemaphoreValue = 4096;
    private int upstreamPullAsyncSemaphoreValue = 4096;
    private int upstreamClientChannelPoolSize = 4;
    private int upstreamClientKeepAliveIntervalSeconds = 30;

    /**
     * 上游（proxy -> broker）TLS 配置，与下游 TLS 独立。
     * 语义与 RocketMQ 客户端 useTLS(true) 一致：开启 TLS 加密传输，
     * 默认不验证 broker 证书（与 RocketMQ 默认 tlsClientAuthServer=false 行为一致）。
     * 当 broker 配置为 tlsMode=enforcing 时必须启用；
     * 当 broker 为 permissive 时可选启用以实现端到端加密。
     */
    private boolean upstreamTlsEnabled = false;
    private String upstreamTlsClientCertPath;
    private String upstreamTlsClientKeyPath;

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
        this.upstreamProducerAsyncSemaphoreValue = upstreamClientAsyncSemaphoreValue;
        this.upstreamPullAsyncSemaphoreValue = upstreamClientAsyncSemaphoreValue;
    }

    public int getUpstreamProducerAsyncSemaphoreValue() {
        return upstreamProducerAsyncSemaphoreValue;
    }

    public void setUpstreamProducerAsyncSemaphoreValue(int upstreamProducerAsyncSemaphoreValue) {
        this.upstreamProducerAsyncSemaphoreValue = upstreamProducerAsyncSemaphoreValue;
    }

    public int getUpstreamPullAsyncSemaphoreValue() {
        return upstreamPullAsyncSemaphoreValue;
    }

    public void setUpstreamPullAsyncSemaphoreValue(int upstreamPullAsyncSemaphoreValue) {
        this.upstreamPullAsyncSemaphoreValue = upstreamPullAsyncSemaphoreValue;
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

    public boolean isUpstreamTlsEnabled() {
        return upstreamTlsEnabled;
    }

    public void setUpstreamTlsEnabled(boolean upstreamTlsEnabled) {
        this.upstreamTlsEnabled = upstreamTlsEnabled;
    }

    public String getUpstreamTlsClientCertPath() {
        return upstreamTlsClientCertPath;
    }

    public void setUpstreamTlsClientCertPath(String upstreamTlsClientCertPath) {
        this.upstreamTlsClientCertPath = upstreamTlsClientCertPath;
    }

    public String getUpstreamTlsClientKeyPath() {
        return upstreamTlsClientKeyPath;
    }

    public void setUpstreamTlsClientKeyPath(String upstreamTlsClientKeyPath) {
        this.upstreamTlsClientKeyPath = upstreamTlsClientKeyPath;
    }
}
