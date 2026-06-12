package com.mq.proxy.core.server;

public class NettyClientConfig {
    private String namesrvAddr = "localhost:9876";
    private int clientOnewaySemaphoreValue = 256;
    private int clientAsyncSemaphoreValue = 64;
    private int connectTimeoutMillis = 3000;
    private int channelNotActiveInterval = 60000;
    private int clientChannelMaxIdleTimeSeconds = 120;
    private int clientKeepAliveIntervalSeconds = 0;
    private int clientKeepAliveRequestCode = 0;
    private int clientWorkerThreadNums = 4;
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    private boolean tlsEnabled = false;
    private String tlsTrustCertPath;
    private String tlsClientCertPath;
    private String tlsClientKeyPath;

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

    public int getClientKeepAliveIntervalSeconds() {
        return clientKeepAliveIntervalSeconds;
    }

    public void setClientKeepAliveIntervalSeconds(int clientKeepAliveIntervalSeconds) {
        this.clientKeepAliveIntervalSeconds = clientKeepAliveIntervalSeconds;
    }

    public int getClientKeepAliveRequestCode() {
        return clientKeepAliveRequestCode;
    }

    public void setClientKeepAliveRequestCode(int clientKeepAliveRequestCode) {
        this.clientKeepAliveRequestCode = clientKeepAliveRequestCode;
    }

    public boolean isClientKeepAliveEnabled() {
        return clientKeepAliveRequestCode > 0 && clientKeepAliveIntervalSeconds > 0;
    }

    public int getClientWorkerThreadNums() {
        return clientWorkerThreadNums;
    }

    public void setClientWorkerThreadNums(int clientWorkerThreadNums) {
        this.clientWorkerThreadNums = clientWorkerThreadNums;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getTlsTrustCertPath() {
        return tlsTrustCertPath;
    }

    public void setTlsTrustCertPath(String tlsTrustCertPath) {
        this.tlsTrustCertPath = tlsTrustCertPath;
    }

    public String getTlsClientCertPath() {
        return tlsClientCertPath;
    }

    public void setTlsClientCertPath(String tlsClientCertPath) {
        this.tlsClientCertPath = tlsClientCertPath;
    }

    public String getTlsClientKeyPath() {
        return tlsClientKeyPath;
    }

    public void setTlsClientKeyPath(String tlsClientKeyPath) {
        this.tlsClientKeyPath = tlsClientKeyPath;
    }
}
