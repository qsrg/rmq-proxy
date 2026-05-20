# proxy-SDK 设计文档

## 1. 背景与目标

### 1.1 背景

当前 mq-proxy 项目已实现单 Proxy 节点的核心消息链路，但客户端连接 Proxy 的方式存在以下问题：

1. **单点故障**：客户端直连单个 Proxy，Proxy 故障时无法自动切换
2. **缺乏高可用**：没有类似 RocketMQ 客户端连接 NameServer 的多地址切换能力
3. **监控缺失**：无法感知 Proxy 的健康状态和性能指标
4. **追踪缺失**：消息全链路追踪能力不足

### 1.2 目标

设计并实现 proxy-SDK，具备以下核心能力：

1. **多 Proxy 高可用切换**：参考 RocketMQ 客户端的 NameServer 切换机制，实现多 Proxy 地址的故障检测和自动切换
2. **监控埋点**：自动收集 Proxy 的成功率、延迟、异常等指标
3. **消息轨迹追踪**：支持消息全链路追踪，便于排查问题
4. **简化 API**：提供简洁易用的 API，降低使用门槛

### 1.3 参考

本设计参考 RocketMQ 4.9.8 客户端的以下实现：

- `NettyRemotingClient.getAndCreateNameserverChannel()` - 多 NameServer 轮询选择和故障切换
- `MQClientAPIImpl.fetchNameServerAddr()` - NameServer 地址动态更新
- `MQClientInstance.cleanOfflineBroker()` - 故障节点清理机制

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                      应用层                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              ProxyClient (简化 API)                   │  │
│  │  - send(topic, message)                              │  │
│  │  - pull(topic, group, queue)                         │  │
│  │  - shutdown()                                        │  │
│  └────────────────────┬─────────────────────────────────┘  │
└───────────────────────┼─────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                   连接管理层                                 │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           ProxyClientFacade                          │   │
│  │  - 管理 Proxy 地址列表                               │   │
│  │  - 故障检测和切换                                    │   │
│  │  - 连接池管理                                        │   │
│  └──────┬───────────────────────────────────┬──────────┘   │
│         │                                   │              │
│  ┌──────▼──────────┐              ┌─────────▼─────────┐   │
│  │ProxyAddressManager│              │ProxyChannelManager│   │
│  │ - 地址列表维护    │              │ - Channel 池      │   │
│  │ - 轮询选择        │              │ - 连接创建/关闭   │   │
│  │ - 故障标记        │              │ - 空闲检测        │   │
│  └──────────────────┘              └───────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                     网络层                                   │
│  ┌─────────────────────────────────────────────────────┐   │
│  │          ProxyRemotingClient                         │   │
│  │  - Netty Bootstrap                                   │   │
│  │  - 协议编解码 (复用 RocketMQ RemotingCommand)        │   │
│  │  - 同步/异步调用                                     │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────┐
│                   监控追踪层                                 │
│  ┌──────────────────────┐    ┌──────────────────────┐     │
│  │   MetricsCollector   │    │   TraceCollector     │     │
│  │  - 成功率统计         │    │  - 轨迹 ID 生成      │     │
│  │  - 延迟统计           │    │  - 全链路记录        │     │
│  │  - 异常统计           │    │  - 上下文传递        │     │
│  └──────────────────────┘    └──────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 模块职责

| 模块 | 职责 | 关键类 |
|------|------|--------|
| **API 层** | 提供简化的客户端 API | `ProxyClient` |
| **连接管理层** | 管理 Proxy 地址、连接池、故障切换 | `ProxyClientFacade`, `ProxyAddressManager`, `ProxyChannelManager` |
| **网络层** | Netty 网络通信、协议编解码 | `ProxyRemotingClient` |
| **监控追踪层** | 指标收集、轨迹追踪 | `MetricsCollector`, `TraceCollector` |

### 2.3 包结构

```
com.mq.proxy.sdk
├── client/                          // API 层
│   ├── ProxyClient.java            // 简化 API 入口
│   ├── ProxyClientConfig.java      // 配置类
│   ├── SendResult.java             // 发送结果
│   └── PullResult.java             // 拉取结果
├── facade/                          // 连接管理层
│   ├── ProxyClientFacade.java      // 核心门面
│   ├── ProxyAddressManager.java    // 地址管理
│   └── ProxyChannelManager.java    // 连接池管理
├── remoting/                        // 网络层
│   ├── ProxyRemotingClient.java    // 网络客户端
│   ├── ProxyClientHandler.java     // Netty Handler
│   └── ProxyResponseFuture.java    // 异步响应 Future
├── monitor/                         // 监控层
│   ├── MetricsCollector.java       // 指标收集
│   └── ProxyMetrics.java           // 指标数据结构
├── trace/                           // 追踪层
│   ├── TraceCollector.java         // 轨迹收集
│   ├── TraceRecord.java            // 轨迹记录
│   └── TraceIdGenerator.java       // 轨迹 ID 生成器
└── exception/                       // 异常
    ├── ProxyException.java         // 基础异常
    ├── ProxyConnectException.java  // 连接异常
    └── ProxyTimeoutException.java  // 超时异常
```

---

## 3. 核心类设计

### 3.1 ProxyClientConfig - 配置类

```java
public class ProxyClientConfig {
    /**
     * Proxy 地址列表（分号分隔）
     * 示例："192.168.1.100:10911;192.168.1.101:10911;192.168.1.102:10911"
     */
    private String proxyAddrs = "127.0.0.1:10911";
    
    /**
     * 连接超时时间（毫秒）
     */
    private int connectTimeoutMillis = 3000;
    
    /**
     * 请求超时时间（毫秒）
     */
    private int requestTimeoutMillis = 3000;
    
    /**
     * 重试次数（故障切换）
     * 当一个 Proxy 失败后，最多尝试多少个不同的 Proxy
     */
    private int retryTimes = 3;
    
    /**
     * 故障隔离时间（毫秒）
     * 连接失败后，该地址在此时间内不会被选择
     */
    private long faultIsolationDurationMillis = 30000;
    
    /**
     * 是否启用监控
     */
    private boolean enableMetrics = true;
    
    /**
     * 是否启用追踪
     */
    private boolean enableTrace = true;
    
    /**
     * Netty worker 线程数
     */
    private int workerThreadNums = Runtime.getRuntime().availableProcessors();
    
    /**
     * 空闲连接检测间隔（毫秒）
     * 定期扫描并关闭空闲连接
     */
    private long idleChannelScanIntervalMillis = 60000;
    
    /**
     * 空闲连接超时时间（毫秒）
     * 超过此时间未使用的连接会被关闭
     */
    private long idleChannelTimeoutMillis = 120000;
}
```

### 3.2 ProxyAddressManager - 地址管理

**核心职责**：
- 维护 Proxy 地址列表
- 轮询选择可用地址
- 故障标记和隔离
- 自动恢复故障地址

```java
public class ProxyAddressManager {
    private final ProxyClientConfig config;
    
    /**
     * Proxy 地址列表（不可变）
     */
    private final List<String> proxyAddrList;
    
    /**
     * 当前选择的地址索引（原子递增，实现轮询）
     */
    private final AtomicInteger index = new AtomicInteger(0);
    
    /**
     * 故障地址表：addr -> 故障时间戳
     */
    private final ConcurrentHashMap<String, Long> faultAddrTable = new ConcurrentHashMap<>();
    
    public ProxyAddressManager(ProxyClientConfig config) {
        this.config = config;
        this.proxyAddrList = parseProxyAddrs(config.getProxyAddrs());
    }
    
    /**
     * 选择一个可用的 Proxy 地址
     * 
     * 算法：
     * 1. 轮询所有地址（AtomicInteger.incrementAndGet() % size）
     * 2. 跳过处于故障隔离期的地址
     * 3. 如果所有地址都在故障隔离期，返回 null
     * 
     * 参考：NettyRemotingClient.getAndCreateNameserverChannel()
     */
    public String selectProxyAddr() {
        if (proxyAddrList.isEmpty()) {
            return null;
        }
        
        // 尝试遍历所有地址
        for (int i = 0; i < proxyAddrList.size(); i++) {
            int currentIndex = Math.abs(index.incrementAndGet()) % proxyAddrList.size();
            String addr = proxyAddrList.get(currentIndex);
            
            // 检查是否在故障隔离期
            if (!isInFaultIsolation(addr)) {
                return addr;
            }
        }
        
        // 所有地址都在故障隔离期
        return null;
    }
    
    /**
     * 标记地址为故障
     * 
     * @param addr Proxy 地址
     */
    public void markFault(String addr) {
        faultAddrTable.put(addr, System.currentTimeMillis());
        log.warn("Proxy address marked as fault: {}", addr);
    }
    
    /**
     * 清除故障标记（连接成功时调用）
     * 
     * @param addr Proxy 地址
     */
    public void clearFault(String addr) {
        Long faultTime = faultAddrTable.remove(addr);
        if (faultTime != null) {
            log.info("Proxy address recovered from fault: {}", addr);
        }
    }
    
    /**
     * 检查地址是否在故障隔离期
     */
    private boolean isInFaultIsolation(String addr) {
        Long faultTime = faultAddrTable.get(addr);
        if (faultTime == null) {
            return false;
        }
        
        long elapsed = System.currentTimeMillis() - faultTime;
        if (elapsed >= config.getFaultIsolationDurationMillis()) {
            // 故障隔离期已过，自动清除
            faultAddrTable.remove(addr);
            log.info("Proxy address fault isolation expired: {}", addr);
            return false;
        }
        
        return true;
    }
    
    /**
     * 解析 Proxy 地址列表
     * 格式："ip1:port1;ip2:port2;ip3:port3"
     */
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
}
```

### 3.3 ProxyChannelManager - 连接池管理

**核心职责**：
- 维护 Channel 连接池
- 创建和关闭连接
- 空闲连接检测

```java
public class ProxyChannelManager {
    private final ProxyClientConfig config;
    private final Bootstrap bootstrap;
    
    /**
     * Channel 池：addr -> ChannelWrapper
     */
    private final ConcurrentHashMap<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();
    
    /**
     * Channel 包装类，包含 Channel 和最后使用时间
     */
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
    
    /**
     * 获取或创建 Channel
     * 
     * 如果 Channel 不存在或已断开，重新建立连接
     * 
     * @param addr Proxy 地址（格式：ip:port）
     * @return 活跃的 Channel
     * @throws ProxyConnectException 连接失败
     */
    public Channel getOrCreateChannel(String addr) throws ProxyConnectException {
        ChannelWrapper cw = channelTable.get(addr);
        if (cw != null && cw.isOK()) {
            cw.updateLastUseTime();
            return cw.getChannel();
        }
        
        // 创建新连接
        return createChannel(addr);
    }
    
    /**
     * 创建新连接
     */
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
            throw new ProxyConnectException(addr, e);
        }
    }
    
    /**
     * 关闭指定 Channel
     */
    public void closeChannel(String addr) {
        ChannelWrapper cw = channelTable.remove(addr);
        if (cw != null && cw.getChannel() != null) {
            cw.getChannel().close();
            log.info("Closed channel to proxy: {}", addr);
        }
    }
    
    /**
     * 扫描并关闭空闲连接
     * 
     * @param timeoutMillis 空闲超时时间
     */
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
    
    /**
     * 关闭所有连接
     */
    public void closeAllChannels() {
        for (String addr : channelTable.keySet()) {
            closeChannel(addr);
        }
    }
}
```

### 3.4 ProxyClientFacade - 核心门面

**核心职责**：
- 协调地址管理、连接管理、网络通信
- 实现故障切换逻辑
- 集成监控和追踪

```java
public class ProxyClientFacade {
    private final ProxyClientConfig config;
    private final ProxyAddressManager addressManager;
    private final ProxyChannelManager channelManager;
    private final ProxyRemotingClient remotingClient;
    private final MetricsCollector metricsCollector;
    private final TraceCollector traceCollector;
    
    private final ScheduledExecutorService scheduledExecutor;
    
    public ProxyClientFacade(ProxyClientConfig config) {
        this.config = config;
        this.addressManager = new ProxyAddressManager(config);
        this.remotingClient = new ProxyRemotingClient(config);
        this.channelManager = new ProxyChannelManager(config, remotingClient.getBootstrap());
        this.metricsCollector = new MetricsCollector(config);
        this.traceCollector = new TraceCollector(config);
        
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        
        // 启动空闲连接扫描
        startIdleChannelScan();
    }
    
    /**
     * 发送请求（带故障切换）
     * 
     * 算法：
     * 1. 选择 Proxy 地址（轮询 + 跳过故障）
     * 2. 获取或创建 Channel
     * 3. 发送请求
     * 4. 成功：清除故障标记，记录指标
     * 5. 失败：标记故障，关闭连接，重试下一个地址
     * 
     * 参考：NettyRemotingClient.getAndCreateNameserverChannel()
     */
    public RemotingCommand invokeSync(RemotingCommand request, long timeoutMillis) 
            throws ProxyException {
        
        int maxRetryTimes = config.getRetryTimes();
        Exception lastException = null;
        
        for (int i = 0; i < maxRetryTimes; i++) {
            // 选择 Proxy 地址
            String proxyAddr = addressManager.selectProxyAddr();
            if (proxyAddr == null) {
                throw new ProxyConnectException("No available proxy address");
            }
            
            long startTime = System.currentTimeMillis();
            
            try {
                // 获取或创建 Channel
                Channel channel = channelManager.getOrCreateChannel(proxyAddr);
                
                // 发送请求
                RemotingCommand response = remotingClient.invokeSync(
                    channel, request, timeoutMillis);
                
                // 成功：清除故障标记
                addressManager.clearFault(proxyAddr);
                
                // 记录监控指标
                long elapsed = System.currentTimeMillis() - startTime;
                metricsCollector.recordSuccess(proxyAddr, elapsed);
                
                return response;
                
            } catch (Exception e) {
                // 失败：标记故障，关闭连接
                addressManager.markFault(proxyAddr);
                channelManager.closeChannel(proxyAddr);
                
                // 记录监控指标
                metricsCollector.recordFailure(proxyAddr, e);
                
                lastException = e;
                log.warn("Request to proxy {} failed, attempt {}/{}, error: {}", 
                    proxyAddr, i + 1, maxRetryTimes, e.getMessage());
            }
        }
        
        // 所有重试都失败
        throw new ProxyException("All proxy addresses failed after " + maxRetryTimes + " attempts", 
            lastException);
    }
    
    /**
     * 启动空闲连接扫描
     */
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
    
    /**
     * 关闭
     */
    public void shutdown() {
        scheduledExecutor.shutdown();
        channelManager.closeAllChannels();
        remotingClient.shutdown();
    }
}
```

### 3.5 ProxyClient - 简化 API

**核心职责**：
- 提供简化的发送/拉取 API
- 构建和解析 RemotingCommand
- 集成轨迹追踪

```java
public class ProxyClient {
    private final ProxyClientConfig config;
    private final ProxyClientFacade facade;
    private final TraceCollector traceCollector;
    
    private volatile boolean started = false;
    
    public ProxyClient(ProxyClientConfig config) {
        this.config = config;
        this.facade = new ProxyClientFacade(config);
        this.traceCollector = new TraceCollector(config);
    }
    
    /**
     * 启动客户端
     */
    public void start() {
        if (!started) {
            facade.start();
            started = true;
            log.info("ProxyClient started, proxy addresses: {}", config.getProxyAddrs());
        }
    }
    
    /**
     * 发送消息
     * 
     * @param topic 主题
     * @param tags 标签
     * @param body 消息体
     * @return 发送结果
     */
    public SendResult send(String topic, String tags, byte[] body) throws ProxyException {
        return send(topic, tags, null, body);
    }
    
    /**
     * 发送消息（带 Key）
     */
    public SendResult send(String topic, String tags, String keys, byte[] body) 
            throws ProxyException {
        
        // 生成轨迹 ID
        String traceId = null;
        if (config.isEnableTrace()) {
            traceId = traceCollector.generateTraceId();
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            // 构建 RemotingCommand（复用 RocketMQ 协议）
            RemotingCommand request = buildSendMessageRequest(topic, tags, keys, body, traceId);
            
            // 发送请求
            RemotingCommand response = facade.invokeSync(request, config.getRequestTimeoutMillis());
            
            // 解析响应
            SendResult result = parseSendResult(response);
            result.setTraceId(traceId);
            
            // 记录轨迹
            if (config.isEnableTrace()) {
                traceCollector.recordSendTrace(traceId, topic, result.getMsgId(), 
                    startTime, true, null);
            }
            
            return result;
            
        } catch (Exception e) {
            // 记录失败轨迹
            if (config.isEnableTrace()) {
                traceCollector.recordSendTrace(traceId, topic, null, 
                    startTime, false, e.getMessage());
            }
            
            throw e;
        }
    }
    
    /**
     * 拉取消息
     */
    public PullResult pull(String topic, String consumerGroup, 
                          int queueId, long offset, int maxNums) throws ProxyException {
        
        // 构建 RemotingCommand
        RemotingCommand request = buildPullMessageRequest(
            topic, consumerGroup, queueId, offset, maxNums);
        
        // 发送请求
        RemotingCommand response = facade.invokeSync(request, config.getRequestTimeoutMillis());
        
        // 解析响应
        return parsePullResult(response);
    }
    
    /**
     * 关闭客户端
     */
    public void shutdown() {
        if (started) {
            facade.shutdown();
            started = false;
            log.info("ProxyClient shutdown");
        }
    }
    
    /**
     * 获取监控数据
     */
    public Map<String, ProxyMetricsSnapshot> getMetrics() {
        return facade.getMetricsCollector().getSnapshot();
    }
    
    // 私有方法：构建请求、解析响应等
    private RemotingCommand buildSendMessageRequest(...) { ... }
    private SendResult parseSendResult(...) { ... }
    private RemotingCommand buildPullMessageRequest(...) { ... }
    private PullResult parsePullResult(...) { ... }
}
```

---

## 4. 故障切换机制详解

### 4.1 切换流程

```
┌─────────────────────────────────────────────────────────────┐
│                    请求发送流程                              │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────┐
        │  选择 Proxy 地址（轮询 + 跳过故障） │
        │  proxyAddr = selectProxyAddr()   │
        └──────────────┬──────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────┐
        │  获取或创建 Channel              │
        │  channel = getOrCreateChannel()  │
        └──────────────┬──────────────────┘
                       │
                       ▼
        ┌─────────────────────────────────┐
        │  发送请求                        │
        │  response = invokeSync()         │
        └──────────────┬──────────────────┘
                       │
            ┌──────────┴──────────┐
            │                     │
            ▼                     ▼
        ┌─────────┐          ┌─────────┐
        │  成功   │          │  失败   │
        └────┬────┘          └────┬────┘
             │                    │
             ▼                    ▼
    ┌────────────────┐    ┌────────────────────┐
    │ 清除故障标记    │    │ 标记地址为故障      │
    │ 记录成功指标    │    │ 关闭 Channel        │
    │ 返回响应       │    │ 记录失败指标        │
    └────────────────┘    └──────────┬─────────┘
                                     │
                                     ▼
                          ┌──────────────────────┐
                          │  重试次数 < 最大值？  │
                          └──────────┬───────────┘
                                     │
                          ┌──────────┴──────────┐
                          │                     │
                          ▼                     ▼
                      ┌───────┐           ┌──────────┐
                      │  是   │           │   否     │
                      └───┬───┘           └────┬─────┘
                          │                    │
                          │                    ▼
                          │           ┌──────────────────┐
                          │           │ 抛出异常          │
                          │           │ "All proxies failed" │
                          │           └──────────────────┘
                          │
                          └──────────────┐
                                         ▼
                              ┌─────────────────────┐
                              │ 选择下一个 Proxy 地址 │
                              │ （跳过故障隔离期地址）│
                              └─────────────────────┘
```

### 4.2 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `retryTimes` | 3 | 最大重试次数（尝试不同的 Proxy） |
| `faultIsolationDurationMillis` | 30000 (30秒) | 故障隔离时长，期间不选择该地址 |
| `connectTimeoutMillis` | 3000 (3秒) | 连接超时时间 |
| `requestTimeoutMillis` | 3000 (3秒) | 请求超时时间 |

### 4.3 故障检测触发条件

以下情况会触发故障检测：

1. **连接失败**：Netty `connect()` 失败
2. **连接断开**：Channel 变为 inactive
3. **请求超时**：请求在指定时间内未收到响应
4. **请求异常**：收到异常响应（如 Proxy 内部错误）

### 4.4 自动恢复机制

故障地址不会永久隔离，而是采用**时间衰减**策略：

- 连接失败后，标记故障时间戳
- 在 `faultIsolationDurationMillis` 时间内，该地址不会被选择
- 超过隔离时间后，故障标记自动清除，重新参与轮询
- 如果再次失败，重新标记故障，隔离时间重新计算

这种机制的好处：
- 避免永久隔离导致可用地址减少
- 自动尝试恢复故障节点
- 适合 Proxy 临时故障后恢复的场景

---

## 5. 监控和追踪设计

### 5.1 MetricsCollector - 监控埋点

```java
public class MetricsCollector {
    private final ProxyClientConfig config;
    
    /**
     * 统计数据：proxyAddr -> ProxyMetrics
     */
    private final ConcurrentHashMap<String, ProxyMetrics> metricsTable = new ConcurrentHashMap<>();
    
    /**
     * 记录成功请求
     */
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
    
    /**
     * 记录失败请求
     */
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
    
    /**
     * 获取监控快照
     */
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
            
            // 计算成功率
            long total = snap.getSuccessCount() + snap.getFailureCount();
            if (total > 0) {
                snap.setSuccessRate((double) snap.getSuccessCount() / total);
            }
            
            // 计算平均延迟
            if (snap.getSuccessCount() > 0) {
                snap.setAvgElapsedMillis(
                    snap.getTotalElapsedMillis() / snap.getSuccessCount());
            }
            
            snapshot.put(addr, snap);
        }
        
        return snapshot;
    }
}

/**
 * Proxy 指标数据结构
 */
public class ProxyMetrics {
    AtomicLong successCount = new AtomicLong(0);
    AtomicLong failureCount = new AtomicLong(0);
    AtomicLong totalElapsedMillis = new AtomicLong(0);
    AtomicLong lastSuccessTime = new AtomicLong(0);
    AtomicLong lastFailureTime = new AtomicLong(0);
    AtomicReference<String> lastException = new AtomicReference<>("");
}

/**
 * 监控快照（不可变）
 */
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
    
    // getters
}
```

### 5.2 TraceCollector - 轨迹追踪

```java
public class TraceCollector {
    private final ProxyClientConfig config;
    private final AtomicLong traceIdSequence = new AtomicLong(0);
    
    /**
     * 轨迹记录队列（异步写入）
     */
    private final BlockingQueue<TraceRecord> traceQueue = new LinkedBlockingQueue<>(10000);
    
    /**
     * 异步写入线程
     */
    private final ExecutorService traceExecutor = Executors.newSingleThreadExecutor();
    
    public TraceCollector(ProxyClientConfig config) {
        this.config = config;
        startTraceWriter();
    }
    
    /**
     * 生成轨迹 ID
     * 
     * 格式：IP + 进程ID + 时间戳 + 序列号
     * 示例：192.168.1.100@12345@1620000000000@0001
     */
    public String generateTraceId() {
        String ip = getLocalIP();
        String pid = getProcessId();
        long timestamp = System.currentTimeMillis();
        long seq = traceIdSequence.incrementAndGet();
        
        return String.format("%s@%s@%d@%04d", ip, pid, timestamp, seq % 10000);
    }
    
    /**
     * 记录发送轨迹
     */
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
        
        // 异步写入
        if (!traceQueue.offer(record)) {
            log.warn("Trace queue is full, drop trace: {}", traceId);
        }
    }
    
    /**
     * 启动异步写入线程
     */
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
    
    /**
     * 写入轨迹（可扩展为写入数据库、追踪系统等）
     */
    private void writeTrace(TraceRecord record) {
        // 默认写入日志
        log.info("Trace: traceId={}, topic={}, msgId={}, success={}, elapsed={}ms",
            record.getTraceId(), record.getTopic(), record.getMsgId(),
            record.isSuccess(), record.getRecordTime() - record.getSendTime());
    }
}

/**
 * 轨迹记录
 */
public class TraceRecord {
    private String traceId;
    private String topic;
    private String msgId;
    private long sendTime;
    private boolean success;
    private String errorMsg;
    private long recordTime;
    
    // getters and setters
}
```

---

## 6. 使用示例

### 6.1 基础使用

```java
// 1. 创建配置
ProxyClientConfig config = new ProxyClientConfig();
config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911;192.168.1.102:10911");
config.setRetryTimes(3);
config.setFaultIsolationDurationMillis(30000);
config.setEnableMetrics(true);
config.setEnableTrace(true);

// 2. 创建客户端
ProxyClient client = new ProxyClient(config);
client.start();

// 3. 发送消息
try {
    SendResult result = client.send("TopicTest", "TagA", "Hello World".getBytes());
    System.out.println("发送成功: msgId=" + result.getMsgId() + ", traceId=" + result.getTraceId());
} catch (ProxyException e) {
    System.err.println("发送失败: " + e.getMessage());
}

// 4. 拉取消息
try {
    PullResult result = client.pull("TopicTest", "ConsumerGroup", 0, 0L, 32);
    for (MessageExt msg : result.getMsgFoundList()) {
        System.out.println("收到消息: " + new String(msg.getBody()));
    }
} catch (ProxyException e) {
    System.err.println("拉取失败: " + e.getMessage());
}

// 5. 获取监控数据
Map<String, ProxyMetricsSnapshot> metrics = client.getMetrics();
for (Map.Entry<String, ProxyMetricsSnapshot> entry : metrics.entrySet()) {
    System.out.println("Proxy: " + entry.getKey());
    System.out.println("  成功率: " + entry.getValue().getSuccessRate());
    System.out.println("  平均延迟: " + entry.getValue().getAvgElapsedMillis() + "ms");
}

// 6. 关闭客户端
client.shutdown();
```

### 6.2 配置说明

```properties
# Proxy 地址列表（分号分隔）
proxy.addrs=192.168.1.100:10911;192.168.1.101:10911;192.168.1.102:10911

# 连接超时时间（毫秒）
proxy.connectTimeout=3000

# 请求超时时间（毫秒）
proxy.requestTimeout=3000

# 重试次数
proxy.retryTimes=3

# 故障隔离时间（毫秒）
proxy.faultIsolationDuration=30000

# 是否启用监控
proxy.enableMetrics=true

# 是否启用追踪
proxy.enableTrace=true

# Netty worker 线程数
proxy.workerThreadNums=4
```

---

## 7. 与 RocketMQ 原生 SDK 的对比

| 维度 | RocketMQ 原生 SDK | proxy-SDK |
|------|-------------------|-----------|
| **连接目标** | 连接 NameServer 发现 Broker | 直接连接 Proxy（Proxy 地址列表） |
| **故障切换** | Broker 级别（不同 brokerName） | Proxy 级别（不同 Proxy 地址） |
| **地址配置** | `namesrvAddr=ip1:port1;ip2:port2` | `proxyAddrs=ip1:port1;ip2:port2` |
| **切换策略** | 轮询 + 故障延迟感知 | 轮询 + 故障隔离 |
| **监控能力** | 基础统计 | 增强监控（成功率、延迟、异常） |
| **追踪能力** | 需额外集成 | 内置轨迹追踪 |
| **API 风格** | 完整的 Producer/Consumer API | 简化的 send/pull API |
| **协议** | RocketMQ 协议 | RocketMQ 协议（复用） |

---

## 8. 实现计划

### 8.1 阶段划分

**阶段 1：核心连接管理（优先级 P0）**
- 实现 `ProxyClientConfig` 配置类
- 实现 `ProxyAddressManager` 地址管理
- 实现 `ProxyChannelManager` 连接池管理
- 实现 `ProxyClientFacade` 核心门面
- 实现 `ProxyRemotingClient` 网络层

**阶段 2：故障切换机制（优先级 P0）**
- 实现轮询选择算法
- 实现故障标记和隔离
- 实现自动恢复机制
- 实现重试逻辑

**阶段 3：监控埋点（优先级 P1）**
- 实现 `MetricsCollector` 指标收集
- 实现成功率、延迟、异常统计
- 实现监控快照 API

**阶段 4：轨迹追踪（优先级 P1）**
- 实现 `TraceCollector` 轨迹收集
- 实现轨迹 ID 生成
- 实现异步写入机制

**阶段 5：简化 API（优先级 P0）**
- 实现 `ProxyClient` 简化 API
- 实现 `SendResult` 和 `PullResult`
- 实现请求构建和响应解析

### 8.2 预估工作量

| 阶段 | 预估代码量 | 预估时间 |
|------|-----------|---------|
| 阶段 1 | 300 行 | 1 天 |
| 阶段 2 | 200 行 | 0.5 天 |
| 阶段 3 | 150 行 | 0.5 天 |
| 阶段 4 | 150 行 | 0.5 天 |
| 阶段 5 | 200 行 | 1 天 |
| **总计** | **约 1000 行** | **约 3.5 天** |

---

## 9. 测试计划

### 9.1 单元测试

- `ProxyAddressManagerTest`：地址选择、故障标记、自动恢复
- `ProxyChannelManagerTest`：连接创建、连接池管理、空闲检测
- `ProxyClientFacadeTest`：故障切换、重试逻辑

### 9.2 集成测试

- `ProxyClientIntegrationTest`：真实 Proxy 连接测试
- `FailoverIntegrationTest`：多 Proxy 故障切换测试

### 9.3 性能测试

- 连接创建性能
- 请求吞吐量
- 故障切换延迟

---

## 10. 风险与注意事项

### 10.1 风险

1. **网络分区**：客户端与部分 Proxy 网络不通，导致误判故障
   - 缓解：合理设置 `faultIsolationDurationMillis`，避免过短

2. **所有 Proxy 故障**：所有 Proxy 都不可用时，客户端无法工作
   - 缓解：增加监控告警，及时发现 Proxy 故障

3. **连接泄漏**：未正确关闭连接，导致资源泄漏
   - 缓解：实现空闲连接检测，定期清理

### 10.2 注意事项

1. **Proxy 地址列表配置**：确保至少配置 2 个 Proxy 地址，才能发挥高可用能力

2. **故障隔离时间**：根据网络环境和 Proxy 恢复速度，合理设置 `faultIsolationDurationMillis`

3. **重试次数**：`retryTimes` 应小于或等于 Proxy 地址数量，否则会重复尝试同一地址

4. **监控数据**：定期调用 `getMetrics()` 获取监控数据，及时发现异常

---

## 11. 后续扩展

### 11.1 短期扩展

- 支持异步发送 API
- 支持批量发送
- 支持消费者 API（完整消费流程）

### 11.2 中期扩展

- 支持配置中心动态获取 Proxy 地址
- 支持通过 NameServer 发现 Proxy
- 支持熔断机制（连续失败达到阈值后熔断）

### 11.3 长期扩展

- 支持自定义故障切换策略
- 支持自定义监控上报（Prometheus、OpenTelemetry）
- 支持自定义轨迹存储（数据库、追踪系统）
