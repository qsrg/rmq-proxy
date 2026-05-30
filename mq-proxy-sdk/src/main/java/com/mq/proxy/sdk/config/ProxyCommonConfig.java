package com.mq.proxy.sdk.config;

/**
 * Proxy公共配置 - 支持链式配置
 */
public class ProxyCommonConfig {

    private String proxyAddrs = "127.0.0.1:19876";

    private int connectTimeoutMillis = 3000;
    private int requestTimeoutMillis = 3000;
    private int retryTimes = 3;

    private long faultIsolationDurationMillis = 30000L;

    private boolean enableMetrics = true;
    private boolean enableTrace = true;

    private int workerThreadNums = Runtime.getRuntime().availableProcessors();

    private long idleChannelScanIntervalMillis = 60000L;
    private long idleChannelTimeoutMillis = 120000L;

    private boolean tlsEnabled = false;
    private String tlsTrustCertPath;
    private String tlsClientCertPath;
    private String tlsClientKeyPath;

    public String getProxyAddrs() {
        return proxyAddrs;
    }

    public ProxyCommonConfig setProxyAddrs(String proxyAddrs) {
        this.proxyAddrs = proxyAddrs;
        return this;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public ProxyCommonConfig setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
        return this;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public ProxyCommonConfig setRequestTimeoutMillis(int requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
        return this;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public ProxyCommonConfig setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public long getFaultIsolationDurationMillis() {
        return faultIsolationDurationMillis;
    }

    public ProxyCommonConfig setFaultIsolationDurationMillis(long faultIsolationDurationMillis) {
        this.faultIsolationDurationMillis = faultIsolationDurationMillis;
        return this;
    }

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public ProxyCommonConfig setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
        return this;
    }

    public boolean isEnableTrace() {
        return enableTrace;
    }

    public ProxyCommonConfig setEnableTrace(boolean enableTrace) {
        this.enableTrace = enableTrace;
        return this;
    }

    public int getWorkerThreadNums() {
        return workerThreadNums;
    }

    public ProxyCommonConfig setWorkerThreadNums(int workerThreadNums) {
        this.workerThreadNums = workerThreadNums;
        return this;
    }

    public long getIdleChannelScanIntervalMillis() {
        return idleChannelScanIntervalMillis;
    }

    public ProxyCommonConfig setIdleChannelScanIntervalMillis(long idleChannelScanIntervalMillis) {
        this.idleChannelScanIntervalMillis = idleChannelScanIntervalMillis;
        return this;
    }

    public long getIdleChannelTimeoutMillis() {
        return idleChannelTimeoutMillis;
    }

    public ProxyCommonConfig setIdleChannelTimeoutMillis(long idleChannelTimeoutMillis) {
        this.idleChannelTimeoutMillis = idleChannelTimeoutMillis;
        return this;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public ProxyCommonConfig setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public String getTlsTrustCertPath() {
        return tlsTrustCertPath;
    }

    public ProxyCommonConfig setTlsTrustCertPath(String tlsTrustCertPath) {
        this.tlsTrustCertPath = tlsTrustCertPath;
        return this;
    }

    public String getTlsClientCertPath() {
        return tlsClientCertPath;
    }

    public ProxyCommonConfig setTlsClientCertPath(String tlsClientCertPath) {
        this.tlsClientCertPath = tlsClientCertPath;
        return this;
    }

    public String getTlsClientKeyPath() {
        return tlsClientKeyPath;
    }

    public ProxyCommonConfig setTlsClientKeyPath(String tlsClientKeyPath) {
        this.tlsClientKeyPath = tlsClientKeyPath;
        return this;
    }
}