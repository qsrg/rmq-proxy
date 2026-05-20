package com.mq.proxy.sdk.monitor;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    
    private final ProxyClientConfig config;
    
    private final ConcurrentHashMap<String, ProxyMetrics> metricsTable = new ConcurrentHashMap<>();
    
    public MetricsCollector(ProxyClientConfig config) {
        this.config = config;
    }
    
    public void recordSuccess(String proxyAddr, long elapsedMillis) {
        if (!config.isEnableMetrics()) {
            return;
        }
        
        ProxyMetrics metrics = metricsTable.computeIfAbsent(
            proxyAddr, k -> new ProxyMetrics());
        
        metrics.successCount.incrementAndGet();
        metrics.totalElapsedMillis.addAndGet(elapsedMillis);
        metrics.lastSuccessTime.set(System.currentTimeMillis());
    }
    
    public void recordFailure(String proxyAddr, Exception e) {
        if (!config.isEnableMetrics()) {
            return;
        }
        
        ProxyMetrics metrics = metricsTable.computeIfAbsent(
            proxyAddr, k -> new ProxyMetrics());
        
        metrics.failureCount.incrementAndGet();
        metrics.lastFailureTime.set(System.currentTimeMillis());
        metrics.lastException.set(e.getClass().getSimpleName());
    }
    
    public Map<String, ProxyMetricsSnapshot> getSnapshot() {
        Map<String, ProxyMetricsSnapshot> snapshot = new HashMap<>();
        
        for (Map.Entry<String, ProxyMetrics> entry : metricsTable.entrySet()) {
            String addr = entry.getKey();
            ProxyMetrics metrics = entry.getValue();
            
            ProxyMetricsSnapshot snap = new ProxyMetricsSnapshot();
            snap.setProxyAddr(addr);
            snap.setSuccessCount(metrics.successCount.get());
            snap.setFailureCount(metrics.failureCount.get());
            snap.setTotalElapsedMillis(metrics.totalElapsedMillis.get());
            snap.setLastSuccessTime(metrics.lastSuccessTime.get());
            snap.setLastFailureTime(metrics.lastFailureTime.get());
            snap.setLastException(metrics.lastException.get());
            
            long total = snap.getSuccessCount() + snap.getFailureCount();
            if (total > 0) {
                snap.setSuccessRate((double) snap.getSuccessCount() / total);
            }
            
            if (snap.getSuccessCount() > 0) {
                snap.setAvgElapsedMillis(
                    snap.getTotalElapsedMillis() / snap.getSuccessCount());
            }
            
            snapshot.put(addr, snap);
        }
        
        return snapshot;
    }
}
