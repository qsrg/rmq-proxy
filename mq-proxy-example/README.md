# MQ Proxy Example 使用指南

`mq-proxy-example` 提供原生 RocketMQ 客户端通过 MQ Proxy 接入 RocketMQ 的示例和压测工具。当前模块不依赖 `mq-proxy-core` 或独立 SDK，只依赖 `rocketmq-client`，用于验证“客户端零改动接入 Proxy”的透明代理场景。

## 目录结构

```text
mq-proxy-example/
├── bin/
│   └── benchmark.sh
├── src/main/java/com/mq/proxy/example/
│   ├── quickstart/
│   │   ├── RocketMQProducerQuickStart.java
│   │   └── RocketMQConsumerQuickStart.java
│   ├── orderly/
│   │   ├── OrderlyProducer.java
│   │   └── OrderlyConsumer.java
│   └── benchmark/
│       ├── BenchmarkOptions.java
│       ├── BenchmarkMetrics.java
│       ├── BenchmarkReportFormatter.java
│       └── RocketMQProxyBenchmark.java
└── pom.xml
```

## 前置条件

1. RocketMQ NameServer 和 Broker 已启动。
2. MQ Proxy 已启动，并且 `proxy.namesrvAddr` 指向真实 RocketMQ NameServer。
3. 示例 Topic 已在 RocketMQ 集群中创建。

默认示例使用以下地址和 Topic：

| 示例 | Proxy 地址 | Topic |
|------|------------|-------|
| `RocketMQProducerQuickStart` | `127.0.0.1:10913` | `QuickStartTopic9` |
| `RocketMQConsumerQuickStart` | `127.0.0.1:10913` | `QuickStartTopic9` |
| `OrderlyProducer` / `OrderlyConsumer` | `127.0.0.1:19876` | `OrderlyTopic` |

如果本机 Proxy 使用默认配置 `proxy.listenPort=19876`，需要先把 quickstart 示例中的 `namesrvAddr` 调整为 `127.0.0.1:19876`，或以 `10913` 端口启动 Proxy。

## 编译

在项目根目录执行：

```bash
mvn package -pl mq-proxy-example -am -DskipTests
```

## 快速开始示例

### 生产者

```bash
cd /Users/wcf/java-project/rmq
mvn exec:java -pl mq-proxy-example \
  -Dexec.mainClass="com.mq.proxy.example.quickstart.RocketMQProducerQuickStart"
```

`RocketMQProducerQuickStart` 使用 `DefaultMQProducer`，关键点是把 `namesrvAddr` 设置为 Proxy 地址：

```java
DefaultMQProducer producer = new DefaultMQProducer("QuickStartProducerGroup");
producer.setNamesrvAddr("127.0.0.1:10913");
producer.start();
```

### 消费者

```bash
cd /Users/wcf/java-project/rmq
mvn exec:java -pl mq-proxy-example \
  -Dexec.mainClass="com.mq.proxy.example.quickstart.RocketMQConsumerQuickStart"
```

`RocketMQConsumerQuickStart` 使用 `DefaultMQPushConsumer` 通过 Proxy 消费 `QuickStartTopic9`。

## 顺序消息示例

先启动消费者：

```bash
cd /Users/wcf/java-project/rmq
mvn exec:java -pl mq-proxy-example \
  -Dexec.mainClass="com.mq.proxy.example.orderly.OrderlyConsumer"
```

再启动生产者：

```bash
mvn exec:java -pl mq-proxy-example \
  -Dexec.mainClass="com.mq.proxy.example.orderly.OrderlyProducer"
```

`OrderlyProducer` 根据订单 ID 选择固定队列，`OrderlyConsumer` 使用 `MessageListenerOrderly` 验证同一队列内的消费顺序。

## 压测工具

`RocketMQProxyBenchmark` 支持两种目标：

- `--target proxy`：原生客户端 `namesrvAddr` 指向 Proxy，用于压测 Proxy。
- `--target direct`：原生客户端 `namesrvAddr` 指向真实 NameServer，用于直连基线对比。

### Proxy 压测

```bash
cd /Users/wcf/java-project/rmq
./mq-proxy-example/bin/benchmark.sh \
  --target proxy \
  --proxyAddrs "127.0.0.1:19876" \
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

### 直连基线

```bash
./mq-proxy-example/bin/benchmark.sh \
  --target direct \
  --namesrvAddrs "127.0.0.1:9876" \
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

### 关键参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--target` | `proxy` | `proxy` 或 `direct` |
| `--proxyAddrs` | - | Proxy 地址，多个用分号分隔；`target=proxy` 时必填 |
| `--namesrvAddrs` | - | RocketMQ NameServer 地址，多个用分号分隔；`target=direct` 时必填 |
| `--topic` | `BenchmarkTopic` | 压测 Topic |
| `--mode` | `mixed` | `produce`、`consume` 或 `mixed` |
| `--producerThreads` | `8` | 发送线程数 |
| `--consumerThreads` | `4` | PushConsumer 消费线程数 |
| `--messageSize` | `1024` | 消息体大小，单位字节 |
| `--warmupSeconds` | `30` | 预热时长，预热数据不计入最终结果 |
| `--durationSeconds` | `300` | 正式统计时长 |
| `--reportIntervalSeconds` | `10` | 控制台周期报告间隔 |
| `--sendTimeoutMillis` | `3000` | 发送超时时间 |
| `--instanceId` | 自动生成 | 分布式压测节点标识 |
| `--output` | `target/benchmark-result.json` | 最终 JSON 结果路径 |

输出指标包含发送 TPS、消费 TPS、失败率、重复消费数、发送 RT 分位和端到端消费延迟分位。多机压测时，给每个进程配置不同的 `--instanceId` 和 `--output`。

## 测试环境压测流程

1. 在项目根目录打包：

   ```bash
   mvn clean package -DskipTests
   ```

2. 在 Proxy 机器部署并启动 `mq-proxy-standalone/target/mq-proxy-1.0.1-SNAPSHOT.tar.gz`：

   ```bash
   tar -zxvf mq-proxy-1.0.1-SNAPSHOT.tar.gz
   cd mq-proxy-1.0.1-SNAPSHOT
   vi conf/proxy.properties
   sh bin/proxy.sh
   ```

3. 创建压测 Topic：

   ```bash
   sh bin/mqadmin updateTopic \
     -n "namesrv1:9876;namesrv2:9876" \
     -c DefaultCluster \
     -t BenchmarkTopic \
     -r 8 \
     -w 8
   ```

4. 在压测机保留以下文件并运行 `benchmark.sh`：

   ```text
   mq-proxy-example/target/classes/
   mq-proxy-example/target/lib/
   mq-proxy-example/bin/benchmark.sh
   ```

5. 使用相同参数分别跑 Proxy 和 direct 基线。判断 Proxy 开销时，重点对比成功 TPS、失败率、发送 RT p99 和消费端到端延迟 p99。

## 常见问题

### 客户端连接失败

- 确认示例里的 `namesrvAddr` 配置的是 Proxy 地址，不是真实 NameServer 地址。
- 确认 Proxy 的 `proxy.host` 是客户端可访问的 IP，不能在远程部署时仍配置为 `127.0.0.1`。
- 确认 Proxy 的 `proxy.namesrvAddr` 能访问真实 RocketMQ NameServer。

### 发送超时或失败率高

- 检查 Broker 负载和 Topic 队列数。
- 适当增大 `sendTimeoutMillis`。
- 检查 Proxy 日志中的上游 in-flight 和 semaphore 指标。
- 压测时逐步提高线程数，避免一次性超过 Broker 或 Proxy 能力。

### 消费没有数据

- 确认生产者和消费者使用同一个 Topic。
- 对 PushConsumer，确认 consumer group 是否已经消费到最新 offset。
- 需要从头消费时，更换新的 consumer group 或清理 RocketMQ 中该 group 的消费进度。
