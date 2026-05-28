package com.mq.proxy.core.server;

public class NettyClientConfig {
    private String namesrvAddr = "localhost:9876";
    private int clientOnewaySemaphoreValue = 256;
    private int clientAsyncSemaphoreValue = 64;
    private int connectTimeoutMillis = 3000;
    private int channelNotActiveInterval = 60000;
    private int clientChannelMaxIdleTimeSeconds = 120;
    private int clientWorkerThreadNums = 4;

    // TLS配置
    private boolean tlsEnabled = false;
    private String tlsTrustStorePath;
    private String tlsTrustStorePassword;
    private String tlsKeyStorePath;
    private String tlsKeyStorePassword;
    private String tlsKeyStoreType = "JKS";

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getClientOnewaySemaphoreValue() {
        return clientOnewaySemaphoreValue;
    }

    public void setClientOnewaySemaphoreValue(int clientOnewaySemaphoreValue) {
        this.clientOnewaySemaphoreValue = clientOnewaySemaphoreValue;
    }

    public int getClientAsyncSemaphoreValue() {
        return clientAsyncSemaphoreValue;
    }

    public void setClientAsyncSemaphoreValue(int clientAsyncSemaphoreValue) {
        this.clientAsyncSemaphoreValue = clientAsyncSemaphoreValue;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getChannelNotActiveInterval() {
        return channelNotActiveInterval;
    }

    public void setChannelNotActiveInterval(int channelNotActiveInterval) {
        this.channelNotActiveInterval = channelNotActiveInterval;
    }

    public int getClientChannelMaxIdleTimeSeconds() {
        return clientChannelMaxIdleTimeSeconds;
    }

    public void setClientChannelMaxIdleTimeSeconds(int clientChannelMaxIdleTimeSeconds) {
        this.clientChannelMaxIdleTimeSeconds = clientChannelMaxIdleTimeSeconds;
    }

    public int getClientWorkerThreadNums() {
        return clientWorkerThreadNums;
    }

    public void setClientWorkerThreadNums(int clientWorkerThreadNums) {
        this.clientWorkerThreadNums = clientWorkerThreadNums;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getTlsTrustStorePath() {
        return tlsTrustStorePath;
    }

    public void setTlsTrustStorePath(String tlsTrustStorePath) {
        this.tlsTrustStorePath = tlsTrustStorePath;
    }

    public String getTlsTrustStorePassword() {
        return tlsTrustStorePassword;
    }

    public void setTlsTrustStorePassword(String tlsTrustStorePassword) {
        this.tlsTrustStorePassword = tlsTrustStorePassword;
    }

    public String getTlsKeyStorePath() {
        return tlsKeyStorePath;
    }

    public void setTlsKeyStorePath(String tlsKeyStorePath) {
        this.tlsKeyStorePath = tlsKeyStorePath;
    }

    public String getTlsKeyStorePassword() {
        return tlsKeyStorePassword;
    }

    public void setTlsKeyStorePassword(String tlsKeyStorePassword) {
        this.tlsKeyStorePassword = tlsKeyStorePassword;
    }

    public String getTlsKeyStoreType() {
        return tlsKeyStoreType;
    }

    public void setTlsKeyStoreType(String tlsKeyStoreType) {
        this.tlsKeyStoreType = tlsKeyStoreType;
    }
}
