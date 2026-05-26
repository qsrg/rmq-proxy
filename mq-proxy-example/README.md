# MQ Proxy Example 使用指南

本项目提供多种示例代码，展示 Proxy SDK 和原生 RocketMQ 客户端的使用方式。

## 目录结构

```
mq-proxy-example/
├── src/main/java/com/mq/proxy/example/
│   ├── quickstart/          # 快速开始示例
│   │   ├── ProxySDKProducerQuickStart.java   # Proxy SDK生产者快速入门
│   │   ├── ProxySDKConsumerQuickStart.java   # Proxy SDK消费者快速入门
│   │   ├── RocketMQProducerQuickStart.java   # 原生RocketMQ生产者快速入门
│   │   ├── RocketMQConsumerQuickStart.java   # 原生RocketMQ消费者快速入门
│   │   ├── ProducerConsumerQuickStart.java   # 完整生产-消费流程示例
│   │   └── ComparisonExample.java            # SDK vs 原生对比
│   ├── producer/            # 生产者示例
│   │   ├── SyncProducer.java               # 同步发送示例
│   │   ├── BatchProducer.java              # 批量发送示例
│   │   └── MultiProxyProducer.java         # 多代理地址示例
│   ├── consumer/            # 消费者示例
│   │   └── PullConsumer.java               # Pull拉取模式示例
│   └── benchmark/           # 性能测试
│       └── BenchmarkProducer.java          # 性能压测示例
└── pom.xml
```

## 快速开始

### 1. 编译项目

```bash
cd /Users/wcf/java-project/rmq
mvn clean package -DskipTests
```

### 2. 运行示例

#### Proxy SDK 快速开始

```bash
# 进入 example 目录
cd mq-proxy-example

# 运行 Proxy SDK 快速开始示例
mvn exec:java -Dexec.mainClass="com.mq.proxy.example.quickstart.ProxySDKQuickStart"
```

**前提条件：**
- 代理服务已启动（默认地址：127.0.0.1:11911）
- Topic 已创建

#### 原生 RocketMQ 快速开始

```bash
# 运行原生 RocketMQ 客户端示例
mvn exec:java -Dexec.mainClass="com.mq.proxy.example.quickstart.RocketMQClientQuickStart"
```

**前提条件：**
- RocketMQ NameServer 已启动（默认地址：127.0.0.1:9876）
- RocketMQ Broker 已启动
- Topic 已创建

### 3. 使用打包的 JAR 运行

```bash
# 打包并复制依赖
mvn clean package

# 运行示例（带依赖）
java -cp target/mq-proxy-example-1.0.0-SNAPSHOT.jar:target/lib/* \
  com.mq.proxy.example.quickstart.ProxySDKQuickStart
```

## 示例详解

### 一、快速开始示例

#### 1. ProxySDKQuickStart

展示 Proxy SDK 的基本使用流程：
- 创建配置（代理地址、生产者组、重试次数等）
- 启动客户端
- 发送单条和批量消息
- 获取监控数据
- 关闭客户端

**关键代码：**
```java
ProxyClientConfig config = new ProxyClientConfig();
config.setProxyAddrs("127.0.0.1:11911");
config.setProducerGroup("QuickStartProducerGroup");

ProxyClient client = new ProxyClient(config);
client.start();

SendResult result = client.send(topic, tags, keys.getBytes(), body.getBytes());
System.out.println("消息ID: " + result.getMsgId());

client.shutdown();
```

#### 2. RocketMQClientQuickStart

展示原生 RocketMQ 客户端通过Proxy的使用：
- 创建生产者
- 配置 Proxy 地址作为 NameServer 地址
- 发送消息（通过Proxy转发到RocketMQ）
- 获取发送结果

**关键代码：**
```java
DefaultMQProducer producer = new DefaultMQProducer("QuickStartProducerGroup");
producer.setNamesrvAddr("127.0.0.1:11911");  // Proxy地址作为NameServer
producer.start();

Message msg = new Message(topic, tags, keys, body.getBytes());
SendResult result = producer.send(msg);

producer.shutdown();
```

#### 3. ComparisonExample

对比两种客户端的特点和适用场景：
- 架构差异（两种方式都通过Proxy访问RocketMQ）
- 配置差异（proxyAddrs vs namesrvAddr，都是Proxy地址）
- 特性对比（SDK自定义重试 vs RocketMQ标准重试）
- 使用场景建议

### 二、生产者示例

#### 1. SyncProducer - 同步发送

展示同步发送消息的方式：
- 发送单条消息并等待结果
- 连续发送多条消息
- 统计成功率和延迟

**适用场景：**
- 需要确保消息发送成功
- 对可靠性要求高的场景

#### 2. BatchProducer - 批量发送

展示批量发送提高效率的方法：
- 循环发送批量消息
- 分批发送大量消息
- 统计 TPS 和延迟

**适用场景：**
- 大量消息批量发送
- 提高网络效率

**最佳实践：**
- 控制批量大小（10-50条）
- 相同Topic和Tags
- 监控失败消息

#### 3. MultiProxyProducer - 多代理地址

展示高可用和故障转移机制：
- 配置多个代理地址
- Round-Robin轮询选择
- 故障隔离机制
- 自动切换故障地址

**适用场景：**
- 高可用要求
- 多机房部署
- 故障容灾

**关键特性：**
- 失败地址隔离30秒
- 自动恢复机制
- 监控统计

### 三、消费者示例

#### PullConsumer - Pull 模式消费

展示主动拉取消息的方式：
- 简单拉取模式
- 长轮询拉取模式（推荐）
- 偏移量管理
- 批量拉取

**适用场景：**
- 需要精确控制消费速率
- 批量处理消息
- 自定义消费进度管理

**最佳实践：**
- 使用长轮询提高效率
- 批量拉取32-100条
- 正确管理偏移量
- 定期持久化进度

### 四、性能测试示例

#### BenchmarkProducer - 性能压测

测试 Proxy SDK 在高并发场景下的性能：
- 多线程并发发送
- 统计 TPS、延迟、成功率
- 监控数据分析

**测试参数：**
- 并发线程数：10
- 每线程消息数：1000
- 总消息数：10000

**性能指标：**
- 平均 TPS
- 平均延迟
- 最大/最小延迟
- 成功率

## 配置说明

### Proxy SDK 配置参数

| 参数 | 默认值 | 说明 |
|-----|--------|------|
| proxyAddrs | 127.0.0.1:10911 | 代理地址（多个用分号分隔） |
| producerGroup | SDKProducerGroup | 生产者组名 |
| retryTimes | 3 | 重试次数 |
| requestTimeoutMillis | 3000 | 请求超时（毫秒） |
| connectTimeoutMillis | 3000 | 连接超时（毫秒） |
| faultIsolationDurationMillis | 30000 | 故障隔离时间（毫秒） |
| suspendTimeoutMillis | 0 | 长轮询超时（毫秒） |
| enableMetrics | true | 启用监控 |
| enableTrace | true | 启用追踪 |

### 原生 RocketMQ 配置参数

| 参数 | 默认值 | 说明 |
|-----|--------|------|
| namesrvAddr | - | **Proxy地址**（作为NameServer代理） |
| producerGroup | - | 生产者组名 |
| retryTimesWhenSendFailed | 2 | 发送失败重试次数 |
| sendMsgTimeout | 3000 | 发送超时（毫秒） |

**重要说明：** 原生RocketMQ客户端配置的namesrvAddr应该是Proxy地址（例如127.0.0.1:11911），Proxy会作为NameServer代理转发请求到后端真实的RocketMQ NameServer。

## 环境准备

### 1. 启动代理服务

```bash
# 启动 mq-proxy-standalone
cd mq-proxy-standalone
mvn exec:java -Dexec.mainClass="com.mq.proxy.standalone.StandaloneApplication"
```

默认配置：
- Proxy端口：11911
- 后端RocketMQ地址：127.0.0.1:9876

### 2. 启动 RocketMQ（后端存储）

如果Proxy连接的是真实的RocketMQ集群，需要启动RocketMQ服务：
```bash
# 启动 NameServer（后端真实NameServer）
nohup sh bin/mqnamesrv &

# 启动 Broker（后端真实Broker）
nohup sh bin/mqbroker -n 127.0.0.1:9876 &
```

**注意：** Proxy服务配置需要指向这些真实的RocketMQ地址。

如果使用Mock存储适配器，则不需要启动RocketMQ。

### 3. 创建 Topic

**使用 Proxy SDK：**
```bash
# 通过管理接口创建（如果支持）
# 或者在RocketMQ端创建
```

**使用 RocketMQ 工具：**
```bash
sh bin/mqadmin updateTopic -t TestTopic -c DefaultCluster -r 4 -w 4
```

## 监控与调优

### Proxy SDK 监控数据

```java
Map<String, ProxyMetricsSnapshot> metrics = client.getMetrics();

for (Map.Entry<String, ProxyMetricsSnapshot> entry : metrics.entrySet()) {
    ProxyMetricsSnapshot snapshot = entry.getValue();
    System.out.println("代理: " + snapshot.getProxyAddr());
    System.out.println("成功: " + snapshot.getSuccessCount());
    System.out.println("失败: " + snapshot.getFailureCount());
    System.out.println("成功率: " + snapshot.getSuccessRate());
    System.out.println("平均延迟: " + snapshot.getAvgElapsedMillis());
}
```

### 性能调优建议

**Producer 侧：**
1. 根据TPS需求调整并发线程数
2. 合理设置重试次数（3-5次）
3. 调整超时时间（内网2s，跨网5s）
4. 配置多个代理地址实现负载均衡
5. 启用监控观察性能瓶颈

**Consumer 侧：**
1. 使用长轮询提高拉取效率
2. 批量拉取消息（32-100条）
3. 正确管理消费偏移量
4. 处理好异常避免无限重试
5. 定期持久化消费进度

## 常见问题

### 1. 连接失败

**Proxy SDK或原生RocketMQ客户端：**
- 检查Proxy服务是否启动（127.0.0.1:11911）
- 检查地址配置是否正确
- Proxy SDK检查proxyAddrs配置
- 原生RocketMQ检查namesrvAddr配置（应该都是Proxy地址）
- 查看日志确认失败原因
- 确认Proxy已连接到后端RocketMQ或Mock存储

### 2. 发送超时

**解决方法：**
- 增加 requestTimeoutMillis
- 检查网络延迟
- 查看Broker负载情况
- 减少并发线程数

### 3. 高失败率

**排查步骤：**
1. 查看监控数据确认失败分布
2. 检查代理或Broker状态
3. 查看错误日志
4. 调整重试策略
5. 检查消息大小是否超限

### 4. 性能不达标

**优化方向：**
1. 增加并发线程数
2. 使用批量发送
3. 配置多个代理地址
4. 调整超时参数
5. 优化网络配置

## 扩展阅读

- [Proxy SDK 重试机制分析](../docs/proxy-sdk-retry-mechanism-analysis.md)
- [Proxy 协议覆盖情况](../memory/proxy_protocol_coverage.md)
- [RocketMQ 官方文档](https://rocketmq.apache.org/docs/)

## 贡献示例

欢迎贡献更多使用示例：

1. 异步发送示例
2. 事务消息示例
3. 延迟消息示例
4. 消息过滤示例
5. 订阅管理示例

---

**版本：** 1.0.0-SNAPSHOT
**更新时间：** 2026-05-22
**维护者：** MQ Proxy Team