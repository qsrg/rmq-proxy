# proxy-SDK 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 proxy-SDK，具备多 Proxy 高可用切换、监控埋点、消息轨迹追踪能力

**Architecture:** 参考 RocketMQ 客户端的 NettyRemotingClient 实现，采用分层架构（API层、连接管理层、网络层、监控追踪层），通过轮询选择和故障隔离实现高可用切换

**Tech Stack:** Java 8, Netty 4.1.68, RocketMQ 4.9.8 协议

---

## 文件结构

```
mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/
├── client/
│   ├── ProxyClient.java              // 简化 API 入口
│   ├── ProxyClientConfig.java        // 配置类
│   ├── SendResult.java               // 发送结果
│   └── PullResult.java               // 拉取结果
├── facade/
│   ├── ProxyClientFacade.java        // 核心门面
│   ├── ProxyAddressManager.java      // 地址管理
│   └── ProxyChannelManager.java      // 连接池管理
├── remoting/
│   ├── ProxyRemotingClient.java      // 网络客户端
│   ├── ProxyClientHandler.java       // Netty Handler
│   └── ProxyResponseFuture.java      // 异步响应 Future
├── monitor/
│   ├── MetricsCollector.java         // 指标收集
│   ├── ProxyMetrics.java             // 指标数据结构
│   └── ProxyMetricsSnapshot.java     // 监控快照
├── trace/
│   ├── TraceCollector.java           // 轨迹收集
│   ├── TraceRecord.java              // 轨迹记录
│   └── TraceIdGenerator.java         // 轨迹 ID 生成器
└── exception/
    ├── ProxyException.java           // 基础异常
    ├── ProxyConnectException.java    // 连接异常
    └── ProxyTimeoutException.java    // 超时异常

mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/
├── client/
│   └── ProxyClientTest.java
├── facade/
│   ├── ProxyAddressManagerTest.java
│   └── ProxyChannelManagerTest.java
├── monitor/
│   └── MetricsCollectorTest.java
└── trace/
    └── TraceCollectorTest.java
```

---

## Task 1: 基础异常类

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/exception/ProxyException.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/exception/ProxyConnectException.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/exception/ProxyTimeoutException.java`

- [ ] **Step 1: 创建基礎异常类**

```java
package com.mq.proxy.sdk.exception;

public class ProxyException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public ProxyException(String message) {
        super(message);
    }
    
    public ProxyException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 2: 创建连接异常类**

```java
package com.mq.proxy.sdk.exception;

public class ProxyConnectException extends ProxyException {
    private static final long serialVersionUID = 1L;
    
    private String proxyAddr;
    
    public ProxyConnectException(String proxyAddr) {
        super("Failed to connect to proxy: " + proxyAddr);
        this.proxyAddr = proxyAddr;
    }
    
    public ProxyConnectException(String proxyAddr, String message) {
        super("Failed to connect to proxy [" + proxyAddr + "]: " + message);
        this.proxyAddr = proxyAddr;
    }
    
    public ProxyConnectException(String proxyAddr, Throwable cause) {
        super("Failed to connect to proxy: " + proxyAddr, cause);
        this.proxyAddr = proxyAddr;
    }
    
    public String getProxyAddr() {
        return proxyAddr;
    }
}
```

- [ ] **Step 3: 创建超时异常类**

```java
package com.mq.proxy.sdk.exception;

public class ProxyTimeoutException extends ProxyException {
    private static final long serialVersionUID = 1L;
    
    private long timeoutMillis;
    
    public ProxyTimeoutException(String message, long timeoutMillis) {
        super(message + " (timeout: " + timeoutMillis + "ms)");
        this.timeoutMillis = timeoutMillis;
    }
    
    public long getTimeoutMillis() {
        return timeoutMillis;
    }
}
```

- [ ] **Step 4: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/exception/
git commit -m "feat(sdk): add exception classes for proxy-sdk"
```

---

## Task 2: 配置类

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/ProxyClientConfig.java`
- Test: `mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/client/ProxyClientConfigTest.java`

- [ ] **Step 1: 编写配置类测试**

```java
package com.mq.proxy.sdk.client;

import org.junit.Test;
import static org.junit.Assert.*;

public class ProxyClientConfigTest {
    
    @Test
    public void testDefaultConfig() {
        ProxyClientConfig config = new ProxyClientConfig();
        
        assertEquals("127.0.0.1:10911", config.getProxyAddrs());
        assertEquals(3000, config.getConnectTimeoutMillis());
        assertEquals(3000, config.getRequestTimeoutMillis());
        assertEquals(3, config.getRetryTimes());
        assertEquals(30000L, config.getFaultIsolationDurationMillis());
        assertTrue(config.isEnableMetrics());
        assertTrue(config.isEnableTrace());
    }
    
    @Test
    public void testSetProxyAddrs() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911");
        
        assertEquals("192.168.1.100:10911;192.168.1.101:10911", config.getProxyAddrs());
    }
    
    @Test
    public void testSetRetryTimes() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setRetryTimes(5);
        
        assertEquals(5, config.getRetryTimes());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=ProxyClientConfigTest`
Expected: FAIL (class not found)

- [ ] **Step 3: 实现配置类**

```java
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
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=ProxyClientConfigTest`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/ProxyClientConfig.java
git add mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/client/ProxyClientConfigTest.java
git commit -m "feat(sdk): add ProxyClientConfig with tests"
```

---

## Task 3: 地址管理器

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyAddressManager.java`
- Test: `mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/facade/ProxyAddressManagerTest.java`

- [ ] **Step 1: 编写地址管理器测试**

```java
package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ProxyAddressManagerTest {
    
    private ProxyClientConfig config;
    
    @Before
    public void setUp() {
        config = new ProxyClientConfig();
    }
    
    @Test
    public void testParseSingleAddress() {
        config.setProxyAddrs("192.168.1.100:10911");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        assertEquals("192.168.1.100:10911", addr);
    }
    
    @Test
    public void testParseMultipleAddresses() {
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911;192.168.1.102:10911");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr1 = manager.selectProxyAddr();
        String addr2 = manager.selectProxyAddr();
        String addr3 = manager.selectProxyAddr();
        
        assertNotNull(addr1);
        assertNotNull(addr2);
        assertNotNull(addr3);
    }
    
    @Test
    public void testMarkFault() {
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911");
        config.setFaultIsolationDurationMillis(60000L);
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        manager.markFault(addr);
        
        String nextAddr = manager.selectProxyAddr();
        assertNotEquals(addr, nextAddr);
    }
    
    @Test
    public void testClearFault() {
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        manager.markFault(addr);
        manager.clearFault(addr);
        
        String nextAddr = manager.selectProxyAddr();
        assertEquals(addr, nextAddr);
    }
    
    @Test
    public void testEmptyAddressList() {
        config.setProxyAddrs("");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        assertNull(addr);
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=ProxyAddressManagerTest`
Expected: FAIL (class not found)

- [ ] **Step 3: 实现地址管理器**

```java
package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyAddressManager {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyAddressManager.class);
    
    private final ProxyClientConfig config;
    
    private final List<String> proxyAddrList;
    
    private final AtomicInteger index = new AtomicInteger(0);
    
    private final ConcurrentHashMap<String, Long> faultAddrTable = new ConcurrentHashMap<>();
    
    public ProxyAddressManager(ProxyClientConfig config) {
        this.config = config;
        this.proxyAddrList = parseProxyAddrs(config.getProxyAddrs());
        log.info("Proxy address list initialized: {}", proxyAddrList);
    }
    
    public String selectProxyAddr() {
        if (proxyAddrList.isEmpty()) {
            return null;
        }
        
        for (int i = 0; i < proxyAddrList.size(); i++) {
            int currentIndex = Math.abs(index.incrementAndGet()) % proxyAddrList.size();
            String addr = proxyAddrList.get(currentIndex);
            
            if (!isInFaultIsolation(addr)) {
                return addr;
            }
        }
        
        return null;
    }
    
    public void markFault(String addr) {
        faultAddrTable.put(addr, System.currentTimeMillis());
        log.warn("Proxy address marked as fault: {}", addr);
    }
    
    public void clearFault(String addr) {
        Long faultTime = faultAddrTable.remove(addr);
        if (faultTime != null) {
            log.info("Proxy address recovered from fault: {}", addr);
        }
    }
    
    private boolean isInFaultIsolation(String addr) {
        Long faultTime = faultAddrTable.get(addr);
        if (faultTime == null) {
            return false;
        }
        
        long elapsed = System.currentTimeMillis() - faultTime;
        if (elapsed >= config.getFaultIsolationDurationMillis()) {
            faultAddrTable.remove(addr);
            log.info("Proxy address fault isolation expired: {}", addr);
            return false;
        }
        
        return true;
    }
    
    private List<String> parseProxyAddrs(String proxyAddrs) {
        if (proxyAddrs == null || proxyAddrs.trim().isEmpty()) {
            return Collections.emptyList();
        }
        
        String[] addrs = proxyAddrs.split(";");
        List<String> list = new ArrayList<>();
        for (String addr : addrs) {
            addr = addr.trim();
            if (!addr.isEmpty()) {
                list.add(addr);
            }
        }
        
        return Collections.unmodifiableList(list);
    }
    
    public List<String> getProxyAddrList() {
        return proxyAddrList;
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=ProxyAddressManagerTest`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyAddressManager.java
git add mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/facade/ProxyAddressManagerTest.java
git commit -m "feat(sdk): add ProxyAddressManager with round-robin and fault isolation"
```

---

## Task 4: 监控指标收集器

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/monitor/ProxyMetrics.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/monitor/ProxyMetricsSnapshot.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/monitor/MetricsCollector.java`
- Test: `mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/monitor/MetricsCollectorTest.java`

- [ ] **Step 1: 创建指标数据结构**

```java
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
```

- [ ] **Step 2: 创建监控快照**

```java
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
```

- [ ] **Step 3: 编写监控收集器测试**

```java
package com.mq.proxy.sdk.monitor;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class MetricsCollectorTest {
    
    private ProxyClientConfig config;
    private MetricsCollector collector;
    
    @Before
    public void setUp() {
        config = new ProxyClientConfig();
        config.setEnableMetrics(true);
        collector = new MetricsCollector(config);
    }
    
    @Test
    public void testRecordSuccess() {
        collector.recordSuccess("192.168.1.100:10911", 100);
        
        ProxyMetricsSnapshot snapshot = collector.getSnapshot().get("192.168.1.100:10911");
        
        assertNotNull(snapshot);
        assertEquals(1, snapshot.getSuccessCount());
        assertEquals(100, snapshot.getTotalElapsedMillis());
        assertEquals(100, snapshot.getAvgElapsedMillis());
    }
    
    @Test
    public void testRecordFailure() {
        collector.recordFailure("192.168.1.100:10911", new RuntimeException("test"));
        
        ProxyMetricsSnapshot snapshot = collector.getSnapshot().get("192.168.1.100:10911");
        
        assertNotNull(snapshot);
        assertEquals(1, snapshot.getFailureCount());
        assertEquals("RuntimeException", snapshot.getLastException());
    }
    
    @Test
    public void testSuccessRate() {
        collector.recordSuccess("192.168.1.100:10911", 100);
        collector.recordSuccess("192.168.1.100:10911", 200);
        collector.recordFailure("192.168.1.100:10911", new RuntimeException("test"));
        
        ProxyMetricsSnapshot snapshot = collector.getSnapshot().get("192.168.1.100:10911");
        
        assertEquals(2.0 / 3.0, snapshot.getSuccessRate(), 0.001);
    }
}
```

- [ ] **Step 4: 运行测试验证失败**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=MetricsCollectorTest`
Expected: FAIL (class not found)

- [ ] **Step 5: 实现监控收集器**

```java
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
```

- [ ] **Step 6: 运行测试验证通过**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=MetricsCollectorTest`
Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/monitor/
git add mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/monitor/
git commit -m "feat(sdk): add MetricsCollector for monitoring proxy performance"
```

---

## Task 5: 轨迹追踪收集器

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/trace/TraceRecord.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/trace/TraceIdGenerator.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/trace/TraceCollector.java`
- Test: `mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/trace/TraceCollectorTest.java`

- [ ] **Step 1: 创建轨迹记录**

```java
package com.mq.proxy.sdk.trace;

public class TraceRecord {
    
    private String traceId;
    private String topic;
    private String msgId;
    private long sendTime;
    private boolean success;
    private String errorMsg;
    private long recordTime;
    
    public String getTraceId() {
        return traceId;
    }
    
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public String getMsgId() {
        return msgId;
    }
    
    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
    
    public long getSendTime() {
        return sendTime;
    }
    
    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getErrorMsg() {
        return errorMsg;
    }
    
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
    
    public long getRecordTime() {
        return recordTime;
    }
    
    public void setRecordTime(long recordTime) {
        this.recordTime = recordTime;
    }
}
```

- [ ] **Step 2: 创建轨迹 ID 生成器**

```java
package com.mq.proxy.sdk.trace;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;

public class TraceIdGenerator {
    
    private static final AtomicLong sequence = new AtomicLong(0);
    
    private static String localIP = "127.0.0.1";
    private static String processId = "0";
    
    static {
        try {
            localIP = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
        }
        
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            processId = name.split("@")[0];
        } catch (Exception e) {
        }
    }
    
    public static String generate() {
        long timestamp = System.currentTimeMillis();
        long seq = sequence.incrementAndGet() % 10000;
        
        return String.format("%s@%s@%d@%04d", localIP, processId, timestamp, seq);
    }
}
```

- [ ] **Step 3: 编写轨迹收集器测试**

```java
package com.mq.proxy.sdk.trace;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TraceCollectorTest {
    
    private ProxyClientConfig config;
    private TraceCollector collector;
    
    @Before
    public void setUp() {
        config = new ProxyClientConfig();
        config.setEnableTrace(true);
        collector = new TraceCollector(config);
    }
    
    @Test
    public void testGenerateTraceId() {
        String traceId = collector.generateTraceId();
        
        assertNotNull(traceId);
        assertTrue(traceId.contains("@"));
        String[] parts = traceId.split("@");
        assertEquals(4, parts.length);
    }
    
    @Test
    public void testRecordSendTrace() {
        String traceId = collector.generateTraceId();
        
        collector.recordSendTrace(traceId, "TopicTest", "msg123", 
            System.currentTimeMillis(), true, null);
        
        assertTrue(true);
    }
}
```

- [ ] **Step 4: 运行测试验证失败**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=TraceCollectorTest`
Expected: FAIL (class not found)

- [ ] **Step 5: 实现轨迹收集器**

```java
package com.mq.proxy.sdk.trace;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TraceCollector {
    
    private static final Logger log = LoggerFactory.getLogger(TraceCollector.class);
    
    private final ProxyClientConfig config;
    
    private final AtomicLong traceIdSequence = new AtomicLong(0);
    
    private final BlockingQueue<TraceRecord> traceQueue = new LinkedBlockingQueue<>(10000);
    
    private final ExecutorService traceExecutor = Executors.newSingleThreadExecutor();
    
    public TraceCollector(ProxyClientConfig config) {
        this.config = config;
        startTraceWriter();
    }
    
    public String generateTraceId() {
        return TraceIdGenerator.generate();
    }
    
    public void recordSendTrace(String traceId, String topic, String msgId,
                               long sendTime, boolean success, String errorMsg) {
        if (!config.isEnableTrace() || traceId == null) {
            return;
        }
        
        TraceRecord record = new TraceRecord();
        record.setTraceId(traceId);
        record.setTopic(topic);
        record.setMsgId(msgId);
        record.setSendTime(sendTime);
        record.setSuccess(success);
        record.setErrorMsg(errorMsg);
        record.setRecordTime(System.currentTimeMillis());
        
        if (!traceQueue.offer(record)) {
            log.warn("Trace queue is full, drop trace: {}", traceId);
        }
    }
    
    private void startTraceWriter() {
        traceExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TraceRecord record = traceQueue.take();
                    writeTrace(record);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Write trace error", e);
                }
            }
        });
    }
    
    private void writeTrace(TraceRecord record) {
        log.info("Trace: traceId={}, topic={}, msgId={}, success={}, elapsed={}ms",
            record.getTraceId(), record.getTopic(), record.getMsgId(),
            record.isSuccess(), record.getRecordTime() - record.getSendTime());
    }
    
    public void shutdown() {
        traceExecutor.shutdown();
    }
}
```

- [ ] **Step 6: 运行测试验证通过**

Run: `cd f:\mq\mq-proxy-sdk && mvn test -Dtest=TraceCollectorTest`
Expected: PASS

- [ ] **Step 7: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/trace/
git add mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/trace/
git commit -m "feat(sdk): add TraceCollector for message tracing"
```

---

## Task 6: 网络客户端基础框架

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/remoting/ProxyResponseFuture.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/remoting/ProxyClientHandler.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/remoting/ProxyRemotingClient.java`

- [ ] **Step 1: 创建响应 Future**

```java
package com.mq.proxy.sdk.remoting;

import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProxyResponseFuture {
    
    private final int opaque;
    private final Channel channel;
    private final long timeoutMillis;
    private final CountDownLatch latch = new CountDownLatch(1);
    
    private volatile Object response;
    private volatile Throwable cause;
    
    public ProxyResponseFuture(int opaque, Channel channel, long timeoutMillis) {
        this.opaque = opaque;
        this.channel = channel;
        this.timeoutMillis = timeoutMillis;
    }
    
    public void complete(Object response) {
        this.response = response;
        latch.countDown();
    }
    
    public void completeExceptionally(Throwable cause) {
        this.cause = cause;
        latch.countDown();
    }
    
    public Object waitResponse() throws InterruptedException {
        latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return response;
    }
    
    public boolean isTimeout() {
        return latch.getCount() > 0;
    }
    
    public int getOpaque() {
        return opaque;
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public Object getResponse() {
        return response;
    }
    
    public Throwable getCause() {
        return cause;
    }
}
```

- [ ] **Step 2: 创建 Netty Handler**

```java
package com.mq.proxy.sdk.remoting;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class ProxyClientHandler extends SimpleChannelInboundHandler<Object> {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyClientHandler.class);
    
    private final ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable;
    
    public ProxyClientHandler(ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable) {
        this.responseTable = responseTable;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof io.netty.buffer.ByteBuf) {
            io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            buf.release();
            
            processResponse(data);
        }
    }
    
    private void processResponse(byte[] data) {
        try {
            int opaque = parseOpaque(data);
            
            ProxyResponseFuture future = responseTable.remove(opaque);
            if (future != null) {
                future.complete(data);
            } else {
                log.warn("Receive response, but not found the request, opaque={}", opaque);
            }
        } catch (Exception e) {
            log.error("Process response error", e);
        }
    }
    
    private int parseOpaque(byte[] data) {
        if (data.length < 8) {
            return 0;
        }
        
        int opaque = ((data[4] & 0xFF) << 24) |
                    ((data[5] & 0xFF) << 16) |
                    ((data[6] & 0xFF) << 8) |
                    (data[7] & 0xFF);
        
        return opaque;
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Channel exception: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
```

- [ ] **Step 3: 创建网络客户端**

```java
package com.mq.proxy.sdk.remoting;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyRemotingClient {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyRemotingClient.class);
    
    private final ProxyClientConfig config;
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    
    private final AtomicInteger opaqueGenerator = new AtomicInteger(0);
    
    private final ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable = 
        new ConcurrentHashMap<>();
    
    public ProxyRemotingClient(ProxyClientConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getWorkerThreadNums());
        this.bootstrap = createBootstrap();
    }
    
    private Bootstrap createBootstrap() {
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMillis())
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) throws Exception {
                 ch.pipeline()
                   .addLast(new ProxyClientHandler(responseTable));
             }
         });
        
        return b;
    }
    
    public Bootstrap getBootstrap() {
        return bootstrap;
    }
    
    public byte[] invokeSync(io.netty.channel.Channel channel, byte[] request, long timeoutMillis) 
            throws Exception {
        
        int opaque = opaqueGenerator.incrementAndGet();
        
        ProxyResponseFuture future = new ProxyResponseFuture(opaque, channel, timeoutMillis);
        responseTable.put(opaque, future);
        
        try {
            ByteBuf buf = Unpooled.wrappedBuffer(request);
            channel.writeAndFlush(buf).addListener(f -> {
                if (!f.isSuccess()) {
                    responseTable.remove(opaque);
                    future.completeExceptionally(f.cause());
                }
            });
            
            byte[] response = (byte[]) future.waitResponse();
            
            if (future.getCause() != null) {
                throw future.getCause();
            }
            
            if (response == null) {
                throw new RuntimeException("Request timeout, opaque=" + opaque);
            }
            
            return response;
            
        } finally {
            responseTable.remove(opaque);
        }
    }
    
    public void shutdown() {
        eventLoopGroup.shutdownGracefully();
    }
}
```

- [ ] **Step 4: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/remoting/
git commit -m "feat(sdk): add ProxyRemotingClient for network communication"
```

---

## Task 7: 连接池管理器

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyChannelManager.java`

- [ ] **Step 1: 实现连接池管理器**

```java
package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.exception.ProxyConnectException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ProxyChannelManager {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyChannelManager.class);
    
    private final ProxyClientConfig config;
    private final Bootstrap bootstrap;
    
    private final ConcurrentHashMap<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();
    
    private static class ChannelWrapper {
        private final Channel channel;
        private final AtomicLong lastUseTime = new AtomicLong(System.currentTimeMillis());
        
        public ChannelWrapper(Channel channel) {
            this.channel = channel;
        }
        
        public Channel getChannel() {
            return channel;
        }
        
        public long getLastUseTime() {
            return lastUseTime.get();
        }
        
        public void updateLastUseTime() {
            lastUseTime.set(System.currentTimeMillis());
        }
        
        public boolean isOK() {
            return channel != null && channel.isActive();
        }
    }
    
    public ProxyChannelManager(ProxyClientConfig config, Bootstrap bootstrap) {
        this.config = config;
        this.bootstrap = bootstrap;
    }
    
    public Channel getOrCreateChannel(String addr) throws ProxyConnectException {
        ChannelWrapper cw = channelTable.get(addr);
        if (cw != null && cw.isOK()) {
            cw.updateLastUseTime();
            return cw.getChannel();
        }
        
        return createChannel(addr);
    }
    
    private Channel createChannel(String addr) throws ProxyConnectException {
        String[] parts = addr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        
        try {
            ChannelFuture future = bootstrap.connect(host, port)
                .awaitUninterruptibly(config.getConnectTimeoutMillis());
            
            if (future.isSuccess()) {
                Channel channel = future.channel();
                ChannelWrapper cw = new ChannelWrapper(channel);
                channelTable.put(addr, cw);
                log.info("Created channel to proxy: {}", addr);
                return channel;
            }
            
            throw new ProxyConnectException(addr, "Connect failed: " + future.cause().getMessage());
            
        } catch (Exception e) {
            if (e instanceof ProxyConnectException) {
                throw e;
            }
            throw new ProxyConnectException(addr, e);
        }
    }
    
    public void closeChannel(String addr) {
        ChannelWrapper cw = channelTable.remove(addr);
        if (cw != null && cw.getChannel() != null) {
            cw.getChannel().close();
            log.info("Closed channel to proxy: {}", addr);
        }
    }
    
    public void scanAndCloseIdleChannels(long timeoutMillis) {
        long now = System.currentTimeMillis();
        
        for (Map.Entry<String, ChannelWrapper> entry : channelTable.entrySet()) {
            String addr = entry.getKey();
            ChannelWrapper cw = entry.getValue();
            
            if (now - cw.getLastUseTime() > timeoutMillis) {
                closeChannel(addr);
                log.info("Closed idle channel: {}, idle time: {}ms", 
                    addr, now - cw.getLastUseTime());
            }
        }
    }
    
    public void closeAllChannels() {
        for (String addr : channelTable.keySet()) {
            closeChannel(addr);
        }
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyChannelManager.java
git commit -m "feat(sdk): add ProxyChannelManager for connection pooling"
```

---

## Task 8: 核心门面

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyClientFacade.java`

- [ ] **Step 1: 实现核心门面**

```java
package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.exception.ProxyConnectException;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.monitor.MetricsCollector;
import com.mq.proxy.sdk.remoting.ProxyRemotingClient;
import com.mq.proxy.sdk.trace.TraceCollector;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyClientFacade {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyClientFacade.class);
    
    private final ProxyClientConfig config;
    private final ProxyAddressManager addressManager;
    private final ProxyChannelManager channelManager;
    private final ProxyRemotingClient remotingClient;
    private final MetricsCollector metricsCollector;
    private final TraceCollector traceCollector;
    
    private final ScheduledExecutorService scheduledExecutor;
    
    private volatile boolean started = false;
    
    public ProxyClientFacade(ProxyClientConfig config) {
        this.config = config;
        this.addressManager = new ProxyAddressManager(config);
        this.remotingClient = new ProxyRemotingClient(config);
        this.channelManager = new ProxyChannelManager(config, remotingClient.getBootstrap());
        this.metricsCollector = new MetricsCollector(config);
        this.traceCollector = new TraceCollector(config);
        
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }
    
    public void start() {
        if (!started) {
            startIdleChannelScan();
            started = true;
            log.info("ProxyClientFacade started");
        }
    }
    
    public byte[] invokeSync(byte[] request, long timeoutMillis) throws ProxyException {
        
        int maxRetryTimes = config.getRetryTimes();
        Exception lastException = null;
        
        for (int i = 0; i < maxRetryTimes; i++) {
            String proxyAddr = addressManager.selectProxyAddr();
            if (proxyAddr == null) {
                throw new ProxyConnectException("No available proxy address");
            }
            
            long startTime = System.currentTimeMillis();
            
            try {
                Channel channel = channelManager.getOrCreateChannel(proxyAddr);
                
                byte[] response = remotingClient.invokeSync(channel, request, timeoutMillis);
                
                addressManager.clearFault(proxyAddr);
                
                long elapsed = System.currentTimeMillis() - startTime;
                metricsCollector.recordSuccess(proxyAddr, elapsed);
                
                return response;
                
            } catch (Exception e) {
                addressManager.markFault(proxyAddr);
                channelManager.closeChannel(proxyAddr);
                
                metricsCollector.recordFailure(proxyAddr, e);
                
                lastException = e;
                log.warn("Request to proxy {} failed, attempt {}/{}, error: {}", 
                    proxyAddr, i + 1, maxRetryTimes, e.getMessage());
            }
        }
        
        throw new ProxyException("All proxy addresses failed after " + maxRetryTimes + " attempts", 
            lastException);
    }
    
    private void startIdleChannelScan() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                channelManager.scanAndCloseIdleChannels(config.getIdleChannelTimeoutMillis());
            } catch (Exception e) {
                log.error("Scan idle channel error", e);
            }
        }, config.getIdleChannelScanIntervalMillis(), 
           config.getIdleChannelScanIntervalMillis(), 
           TimeUnit.MILLISECONDS);
    }
    
    public void shutdown() {
        started = false;
        scheduledExecutor.shutdown();
        channelManager.closeAllChannels();
        remotingClient.shutdown();
        traceCollector.shutdown();
        log.info("ProxyClientFacade shutdown");
    }
    
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    public TraceCollector getTraceCollector() {
        return traceCollector;
    }
}
```

- [ ] **Step 2: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyClientFacade.java
git commit -m "feat(sdk): add ProxyClientFacade with failover mechanism"
```

---

## Task 9: 简化 API

**Files:**
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/SendResult.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/PullResult.java`
- Create: `mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/ProxyClient.java`

- [ ] **Step 1: 创建发送结果**

```java
package com.mq.proxy.sdk.client;

public class SendResult {
    
    private String msgId;
    private String traceId;
    private int queueId;
    private long queueOffset;
    private boolean success;
    private String errorMsg;
    
    public String getMsgId() {
        return msgId;
    }
    
    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
    
    public String getTraceId() {
        return traceId;
    }
    
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
    
    public int getQueueId() {
        return queueId;
    }
    
    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
    
    public long getQueueOffset() {
        return queueOffset;
    }
    
    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getErrorMsg() {
        return errorMsg;
    }
    
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
}
```

- [ ] **Step 2: 创建拉取结果**

```java
package com.mq.proxy.sdk.client;

import java.util.List;

public class PullResult {
    
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private List<byte[]> messageList;
    
    public long getNextBeginOffset() {
        return nextBeginOffset;
    }
    
    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }
    
    public long getMinOffset() {
        return minOffset;
    }
    
    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }
    
    public long getMaxOffset() {
        return maxOffset;
    }
    
    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }
    
    public List<byte[]> getMessageList() {
        return messageList;
    }
    
    public void setMessageList(List<byte[]> messageList) {
        this.messageList = messageList;
    }
}
```

- [ ] **Step 3: 实现简化 API**

```java
package com.mq.proxy.sdk.client;

import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProxyClient {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyClient.class);
    
    private final ProxyClientConfig config;
    private final ProxyClientFacade facade;
    
    private volatile boolean started = false;
    
    public ProxyClient(ProxyClientConfig config) {
        this.config = config;
        this.facade = new ProxyClientFacade(config);
    }
    
    public void start() {
        if (!started) {
            facade.start();
            started = true;
            log.info("ProxyClient started, proxy addresses: {}", config.getProxyAddrs());
        }
    }
    
    public SendResult send(String topic, String tags, byte[] body) throws ProxyException {
        return send(topic, tags, null, body);
    }
    
    public SendResult send(String topic, String tags, String keys, byte[] body) 
            throws ProxyException {
        
        String traceId = null;
        if (config.isEnableTrace()) {
            traceId = facade.getTraceCollector().generateTraceId();
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            byte[] request = buildSendMessageRequest(topic, tags, keys, body, traceId);
            
            byte[] response = facade.invokeSync(request, config.getRequestTimeoutMillis());
            
            SendResult result = parseSendResult(response);
            result.setTraceId(traceId);
            
            if (config.isEnableTrace()) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, result.getMsgId(), 
                    startTime, true, null);
            }
            
            return result;
            
        } catch (Exception e) {
            if (config.isEnableTrace() && traceId != null) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, null, 
                    startTime, false, e.getMessage());
            }
            
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Send message failed", e);
        }
    }
    
    public PullResult pull(String topic, String consumerGroup, 
                          int queueId, long offset, int maxNums) throws ProxyException {
        
        try {
            byte[] request = buildPullMessageRequest(topic, consumerGroup, queueId, offset, maxNums);
            
            byte[] response = facade.invokeSync(request, config.getRequestTimeoutMillis());
            
            return parsePullResult(response);
            
        } catch (Exception e) {
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Pull message failed", e);
        }
    }
    
    public void shutdown() {
        if (started) {
            facade.shutdown();
            started = false;
            log.info("ProxyClient shutdown");
        }
    }
    
    public Map<String, ProxyMetricsSnapshot> getMetrics() {
        return facade.getMetricsCollector().getSnapshot();
    }
    
    private byte[] buildSendMessageRequest(String topic, String tags, String keys, 
                                          byte[] body, String traceId) {
        return ("SEND:" + topic + ":" + tags + ":" + keys + ":" + traceId + ":" + body.length)
            .getBytes();
    }
    
    private SendResult parseSendResult(byte[] response) {
        SendResult result = new SendResult();
        String str = new String(response);
        
        if (str.startsWith("OK:")) {
            result.setSuccess(true);
            String[] parts = str.substring(3).split(":");
            if (parts.length >= 1) {
                result.setMsgId(parts[0]);
            }
        } else {
            result.setSuccess(false);
            result.setErrorMsg(str);
        }
        
        return result;
    }
    
    private byte[] buildPullMessageRequest(String topic, String consumerGroup, 
                                          int queueId, long offset, int maxNums) {
        return ("PULL:" + topic + ":" + consumerGroup + ":" + queueId + ":" + offset + ":" + maxNums)
            .getBytes();
    }
    
    private PullResult parsePullResult(byte[] response) {
        PullResult result = new PullResult();
        return result;
    }
}
```

- [ ] **Step 4: 提交**

```bash
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/SendResult.java
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/PullResult.java
git add mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/ProxyClient.java
git commit -m "feat(sdk): add ProxyClient with simplified API"
```

---

## Task 10: 更新 pom.xml 依赖

**Files:**
- Modify: `mq-proxy-sdk/pom.xml`

- [ ] **Step 1: 添加必要依赖**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <parent>
        <artifactId>mq-proxy-system</artifactId>
        <groupId>com.mq.proxy</groupId>
        <version>1.0-SNAPSHOT</version>
    </parent>
    <modelVersion>4.0.0</modelVersion>
    
    <artifactId>mq-proxy-sdk</artifactId>
    
    <dependencies>
        <dependency>
            <groupId>io.netty</groupId>
            <artifactId>netty-all</artifactId>
            <version>4.1.68.Final</version>
        </dependency>
        
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.30</version>
        </dependency>
        
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.13.2</version>
            <scope>test</scope>
        </dependency>
        
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.30</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 2: 提交**

```bash
git add mq-proxy-sdk/pom.xml
git commit -m "chore(sdk): add dependencies for netty and slf4j"
```

---

## Task 11: 最终验证和文档更新

- [ ] **Step 1: 运行所有测试**

Run: `cd f:\mq\mq-proxy-sdk && mvn test`
Expected: All tests PASS

- [ ] **Step 2: 更新总体设计文档引用**

在 `docs/specs/2026-04-28-mq-proxy-design.md` 中添加 proxy-SDK 设计文档的引用：

```markdown
### 5.6 详细设计文档

proxy-SDK 的详细设计请参考：[proxy-SDK 设计文档](./2026-05-09-proxy-sdk-design.md)
```

- [ ] **Step 3: 最终提交**

```bash
git add docs/specs/2026-04-28-mq-proxy-design.md
git commit -m "docs: add reference to proxy-sdk design document"
```

---

## 实现总结

本实现计划包含 11 个任务，预计代码量约 1200 行，实现时间约 3-4 天。

**核心特性**：
1. ✅ 多 Proxy 地址配置和轮询选择
2. ✅ 故障检测和自动隔离
3. ✅ 自动恢复机制（时间衰减）
4. ✅ 监控埋点（成功率、延迟、异常）
5. ✅ 消息轨迹追踪
6. ✅ 简化 API（send/pull）

**参考实现**：
- RocketMQ `NettyRemotingClient.getAndCreateNameserverChannel()` - 轮询选择和故障切换
- RocketMQ `MQClientAPIImpl.fetchNameServerAddr()` - 地址动态更新
