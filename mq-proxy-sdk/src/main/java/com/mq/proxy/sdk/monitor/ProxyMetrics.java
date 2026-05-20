package com.mq.proxy.sdk.monitor;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ProxyMetrics {
    
    AtomicLong successCount = new AtomicLong(0);
    AtomicLong failureCount = new AtomicLong(0);
    AtomicLong totalElapsedMillis = new AtomicLong(0);
    AtomicLong lastSuccessTime = new AtomicLong(0);
    AtomicLong lastFailureTime = new AtomicLong(0);
    AtomicReference<String> lastException = new AtomicReference<>("");
}
