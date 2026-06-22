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
│       └── RocketMQProxyBenchmark.java     # 原生客户端压测入口
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

#### RocketMQProxyBenchmark - 原生客户端压测

使用原生 RocketMQ Client 压测 Proxy 或直连 NameServer 基线。脚本不绕过 NameServer 协议：

- `--target proxy`：客户端 `namesrvAddr` 使用 `--proxyAddrs`，用于压测 Proxy。
- `--target direct`：客户端 `namesrvAddr` 使用 `--namesrvAddrs`，用于直连 RocketMQ NameServer 做基线对比。
- 多台压测机运行同一命令并配置不同 `--instanceId`，即可做分布式压测。

**Proxy 压测示例：**

```bash
cd /Users/wcf/java-project/rmq
./mq-proxy-example/bin/benchmark.sh \
  --target proxy \
  --proxyAddrs "127.0.0.1:10913" \
  --topic BenchmarkTopic \
  --mode mixed \
  --producerThreads 16 \
  --consumerThreads 8 \
  --messageSize 1024 \
  --warmupSeconds 30 \
  --durationSeconds 300 \
  --reportIntervalSeconds 10 \
  --instanceId node-a \
  --output target/benchmark-proxy-node-a.json
```

**直连 RocketMQ NameServer 基线示例：**

```bash
./mq-proxy-example/bin/benchmark.sh \
  --target direct \
  --namesrvAddrs "10.0.0.1:9876;10.0.0.2:9876" \
  --topic BenchmarkTopic \
  --mode mixed \
  --producerThreads 16 \
  --consumerThreads 8 \
  --messageSize 1024 \
  --warmupSeconds 30 \
  --durationSeconds 300 \
  --instanceId direct-node-a \
  --output target/benchmark-direct-node-a.json
```

**关键参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--target` | `proxy` | `proxy` 或 `direct` |
| `--proxyAddrs` | - | Proxy 地址，多个用分号分隔；`target=proxy` 时必填 |
| `--namesrvAddrs` | - | RocketMQ NameServer 地址，多个用分号分隔；`target=direct` 时必填 |
| `--mode` | `mixed` | `produce`、`consume`、`mixed` |
| `--producerThreads` | `8` | 发送线程数 |
| `--consumerThreads` | `4` | PushConsumer 消费线程数 |
| `--messageSize` | `1024` | 消息体大小，单位字节 |
| `--warmupSeconds` | `30` | 预热时长，预热数据不计入最终结果 |
| `--durationSeconds` | `300` | 正式统计时长 |
| `--reportIntervalSeconds` | `10` | 控制台周期报告间隔 |
| `--instanceId` | 自动生成 | 分布式压测节点标识 |
| `--output` | `target/benchmark-result.json` | 最终 JSON 结果路径 |

**输出指标：**

- 生产：成功 TPS、成功数、失败数、错误率、发送 RT p50/p90/p99/max。
- 消费：成功 TPS、成功数、失败数、重复数、消费处理 RT、端到端消费延迟 p50/p90/p99/max。
- 最终结果会写入 JSON 文件，便于多节点汇总。

#### 测试环境压测流程

测试环境建议把 Proxy 服务和压测客户端分开部署：Proxy 机器只运行 `mq-proxy-standalone`，压测机只运行 `mq-proxy-example` 的 benchmark 脚本。多台压测机可以同时连接同一组 Proxy 地址，用不同 `--instanceId` 区分结果。

**1. 打包 Proxy 和压测客户端**

在项目根目录执行：

```bash
mvn clean package -DskipTests
```

主要产物：

```text
mq-proxy-standalone/target/mq-proxy-1.0.1-SNAPSHOT.tar.gz
mq-proxy-example/target/classes/
mq-proxy-example/target/lib/
mq-proxy-example/bin/benchmark.sh
```

**2. 部署并启动 Proxy**

把 `mq-proxy-standalone/target/mq-proxy-1.0.1-SNAPSHOT.tar.gz` 上传到 Proxy 机器，解压：

```bash
tar -zxvf mq-proxy-1.0.1-SNAPSHOT.tar.gz
cd mq-proxy
```

修改 `conf/proxy.properties`：

```properties
proxy.listenPort=10913
proxy.host=测试环境Proxy机器IP
proxy.namesrvAddr=namesrv1:9876;namesrv2:9876
proxy.workerThreadNums=8
proxy.requestProcessorThreadNums=16
proxy.pullExecutorThreadNums=32
proxy.upstreamClientAsyncSemaphoreValue=4096
proxy.upstreamClientChannelPoolSize=4
proxy.upstreamClientKeepAliveIntervalSeconds=30
```

`SEND_MESSAGE` 使用异步方式转发到 broker，只有 broker 返回最终结果后 Proxy 才向客户端返回成功或失败，因此不会改变客户端同步发送、同步刷盘和同步复制语义。`proxy.requestProcessorThreadNums` 只负责请求解析和发起上游调用，不再同步等待 broker，可以保持在 `CPU 核心数 x 2` 左右。

Proxy 到 broker 的异步在途请求由 `proxy.upstreamClientAsyncSemaphoreValue` 限制，默认 `4096`。压测时如果出现异步并发额度耗尽，应结合 Proxy 内存、broker RT 和客户端超时调整该值，避免无限堆积。

需要临时调整请求处理线程时，可以不改配置文件：

```bash
JAVA_OPT="-Dproxy.requestProcessorThreadNums=64" sh bin/proxy.sh start
```

启动并检查日志：

```bash
sh bin/proxy.sh start
tail -f logs/proxy.log
```

日志中看到 `Proxy started successfully` 表示启动完成。

**3. 创建压测 Topic**

先确认 RocketMQ 集群名：

```bash
sh bin/mqadmin clusterList -n "namesrv1:9876;namesrv2:9876"
```

创建压测 Topic：

```bash
sh bin/mqadmin updateTopic \
  -n "namesrv1:9876;namesrv2:9876" \
  -c DefaultCluster \
  -t BenchmarkTopic \
  -r 8 \
  -w 8
```

如果集群名不是 `DefaultCluster`，把 `-c` 改成实际集群名。生产和消费队列数量建议按压测规模调整，第一轮可以用 `8` 或 `16`。

**4. 部署压测客户端**

把以下内容上传到压测机，保持相对目录结构：

```text
mq-proxy-example/target/classes/
mq-proxy-example/target/lib/
mq-proxy-example/bin/benchmark.sh
```

也可以在压测机拉取代码后直接打包：

```bash
mvn package -pl mq-proxy-example -DskipTests
```

**5. 运行 Proxy 压测**

```bash
./mq-proxy-example/bin/benchmark.sh \
  --target proxy \
  --proxyAddrs "proxy1:10913;proxy2:10913" \
  --topic BenchmarkTopic \
  --mode mixed \
  --producerThreads 16 \
  --consumerThreads 8 \
  --messageSize 1024 \
  --warmupSeconds 30 \
  --durationSeconds 300 \
  --reportIntervalSeconds 10 \
  --instanceId proxy-node-a \
  --output target/benchmark-proxy-node-a.json
```

分布式压测时，在多台压测机同时执行同一命令，并分别设置：

```bash
--instanceId proxy-node-b --output target/benchmark-proxy-node-b.json
--instanceId proxy-node-c --output target/benchmark-proxy-node-c.json
```

**6. 运行直连 NameServer 基线**

为了评估 Proxy 额外开销，建议用相同参数再跑一轮直连基线：

```bash
./mq-proxy-example/bin/benchmark.sh \
  --target direct \
  --namesrvAddrs "namesrv1:9876;namesrv2:9876" \
  --topic BenchmarkTopic \
  --mode mixed \
  --producerThreads 16 \
  --consumerThreads 8 \
  --messageSize 1024 \
  --warmupSeconds 30 \
  --durationSeconds 300 \
  --reportIntervalSeconds 10 \
  --instanceId direct-node-a \
  --output target/benchmark-direct-node-a.json
```

**7. 汇总和判断**

每个 JSON 文件都包含本节点的 `sendSuccessTps`、`consumeSuccessTps`、错误率和延迟分位。多节点结果可以按以下口径汇总：

- 总发送 TPS：所有节点 `sendSuccessTps` 相加。
- 总消费 TPS：所有节点 `consumeSuccessTps` 相加。
- 错误率：用所有节点失败数除以总请求数。
- 延迟分位：单节点 JSON 不能直接合并为全局 p99，正式报告应保留每个节点 p99，并取最差节点作为保守参考。

上线前建议至少跑三组：

- 小流量：`producerThreads=4`、`consumerThreads=2`、`durationSeconds=300`
- 目标流量：按预期生产 TPS 设置线程数，`durationSeconds=1800`
- 极限流量：逐步增加线程数，直到错误率升高或 p99 明显恶化

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
