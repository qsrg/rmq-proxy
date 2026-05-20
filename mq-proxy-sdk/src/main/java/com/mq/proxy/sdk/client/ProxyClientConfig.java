package com.mq.proxy.sdk.client;

public class ProxyClientConfig {
    
    private String proxyAddrs = "127.0.0.1:10911";
    
    private int connectTimeoutMillis = 3000;
    
    private int requestTimeoutMillis = 3000;
    
    private int retryTimes = 3;
    
    private long faultIsolationDurationMillis = 30000L;
    
    private boolean enableMetrics = true;
    
    private boolean enableTrace = true;
    
    private int workerThreadNums = Runtime.getRuntime().availableProcessors();
    
    private long idleChannelScanIntervalMillis = 60000L;
    
    private long idleChannelTimeoutMillis = 120000L;
    
    public String getProxyAddrs() {
        return proxyAddrs;
    }
    
    public void setProxyAddrs(String proxyAddrs) {
        this.proxyAddrs = proxyAddrs;
    }
    
    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }
    
    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }
    
    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }
    
    public void setRequestTimeoutMillis(int requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
    }
    
    public int getRetryTimes() {
        return retryTimes;
    }
    
    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }
    
    public long getFaultIsolationDurationMillis() {
        return faultIsolationDurationMillis;
    }
    
    public void setFaultIsolationDurationMillis(long faultIsolationDurationMillis) {
        this.faultIsolationDurationMillis = faultIsolationDurationMillis;
    }
    
    public boolean isEnableMetrics() {
        return enableMetrics;
    }
    
    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }
    
    public boolean isEnableTrace() {
        return enableTrace;
    }
    
    public void setEnableTrace(boolean enableTrace) {
        this.enableTrace = enableTrace;
    }
    
    public int getWorkerThreadNums() {
        return workerThreadNums;
    }
    
    public void setWorkerThreadNums(int workerThreadNums) {
        this.workerThreadNums = workerThreadNums;
    }
    
    public long getIdleChannelScanIntervalMillis() {
        return idleChannelScanIntervalMillis;
    }
    
    public void setIdleChannelScanIntervalMillis(long idleChannelScanIntervalMillis) {
        this.idleChannelScanIntervalMillis = idleChannelScanIntervalMillis;
    }
    
    public long getIdleChannelTimeoutMillis() {
        return idleChannelTimeoutMillis;
    }
    
    public void setIdleChannelTimeoutMillis(long idleChannelTimeoutMillis) {
        this.idleChannelTimeoutMillis = idleChannelTimeoutMillis;
    }
}
