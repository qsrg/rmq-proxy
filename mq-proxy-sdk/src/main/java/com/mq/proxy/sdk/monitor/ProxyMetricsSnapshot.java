package com.mq.proxy.sdk.monitor;

public class ProxyMetricsSnapshot {
    
    private String proxyAddr;
    private long successCount;
    private long failureCount;
    private long totalElapsedMillis;
    private long lastSuccessTime;
    private long lastFailureTime;
    private String lastException;
    private double successRate;
    private long avgElapsedMillis;
    
    public String getProxyAddr() {
        return proxyAddr;
    }
    
    public void setProxyAddr(String proxyAddr) {
        this.proxyAddr = proxyAddr;
    }
    
    public long getSuccessCount() {
        return successCount;
    }
    
    public void setSuccessCount(long successCount) {
        this.successCount = successCount;
    }
    
    public long getFailureCount() {
        return failureCount;
    }
    
    public void setFailureCount(long failureCount) {
        this.failureCount = failureCount;
    }
    
    public long getTotalElapsedMillis() {
        return totalElapsedMillis;
    }
    
    public void setTotalElapsedMillis(long totalElapsedMillis) {
        this.totalElapsedMillis = totalElapsedMillis;
    }
    
    public long getLastSuccessTime() {
        return lastSuccessTime;
    }
    
    public void setLastSuccessTime(long lastSuccessTime) {
        this.lastSuccessTime = lastSuccessTime;
    }
    
    public long getLastFailureTime() {
        return lastFailureTime;
    }
    
    public void setLastFailureTime(long lastFailureTime) {
        this.lastFailureTime = lastFailureTime;
    }
    
    public String getLastException() {
        return lastException;
    }
    
    public void setLastException(String lastException) {
        this.lastException = lastException;
    }
    
    public double getSuccessRate() {
        return successRate;
    }
    
    public void setSuccessRate(double successRate) {
        this.successRate = successRate;
    }
    
    public long getAvgElapsedMillis() {
        return avgElapsedMillis;
    }
    
    public void setAvgElapsedMillis(long avgElapsedMillis) {
        this.avgElapsedMillis = avgElapsedMillis;
    }
}
