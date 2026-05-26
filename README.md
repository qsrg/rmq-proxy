# MQ Proxy

基于 Netty 的 RocketMQ 透明代理系统，在客户端与 RocketMQ 集群之间提供协议代理、路由虚拟化、可插拔存储后端等能力。

## 架构概览

MQ Proxy 支持两种接入方式：

1. **透明代理模式** — 原生 RocketMQ 客户端只需将 `namesrvAddr` 设为代理地址，代理拦截所有协议请求并通过 `StorageAdapter` 转发至真实 Broker
2. **SDK 模式** — 应用使用 `ProxyClient` SDK 直连代理，自带连接管理、重试、故障隔离、指标采集与链路追踪

核心扩展点为 `StorageAdapter` SPI：通过 Java ServiceLoader 注册新的存储后端（RocketMQ、Mock、或自定义），支持按 Topic 路由到不同适配器。

## 模块介绍

### mq-proxy-core

核心模块，包含协议编解码、Netty 服务器/客户端、请求分发引擎、路由虚拟化与存储抽象层。

| 关键类 | 说明 |
|--------|------|
| `ProxyStartup` | 启动入口，组装所有组件 |
| `ProxyConfig` / `ProxyConfigLoader` | 代理配置及加载 |
| `RemotingCommand` / `RemotingCommandEncoder` / `RemotingCommandDecoder` | RocketMQ 协议帧的编解码 |
| `NettyRemotingServer` | Netty TCP 服务器，管理 Channel 与 Processor 注册 |
| `NettyRemotingClient` | 内部 Netty 客户端，用于转发请求到真实 Broker |
| `MessageEngine` | 中央调度：按 Topic 将请求路由到对应 `StorageAdapter` |
| `ProcessorRegister` | 注册所有请求处理器 |
| `NameServerProcessor` | 处理路由查询，返回虚拟化路由（代理地址替代真实 Broker 地址） |
| `SendMessageProcessor` / `PullMessageProcessor` / `ConsumerManageProcessor` | 消息发送、拉取、消费偏移量处理器 |
| `ClientManageProcessor` | 心跳、客户端注销、消费者列表变更通知与重平衡 |
| `VirtualRouteManager` | 查询真实 NameServer 路由并缓存，生成指向代理的虚拟路由 |
| `ClientConnectionManager` | 管理连接端的生产者/消费者元数据、心跳与过期检测 |
| `StorageAdapter` | 可插拔存储接口（SPI 扩展点） |
| `StorageAdapterManager` | 通过 ServiceLoader 发现适配器并按 Topic 路由 |

**外部依赖**：Netty、SLF4J、Logback、Jackson

### mq-proxy-rocketmq

RocketMQ 存储适配器实现，通过 `NettyRemotingClient` 以原生 RemotingCommand 协议转发请求到真实 Broker。

| 关键类 | 说明 |
|--------|------|
| `RocketMQStorageAdapter` | 实现 `StorageAdapter`，构造 RemotingCommand 请求转发到 Broker 并翻译响应 |
| `RocketMQMessageDecoder` | 从 RocketMQ 二进制格式反序列化消息为 `InternalMessage` |

**模块依赖**：mq-proxy-core

### mq-proxy-mock

内存 Mock 存储适配器，用于测试和开发，无需真实 Broker。

| 关键类 | 说明 |
|--------|------|
| `MockStorageAdapter` | 基于 `ConcurrentHashMap` 的内存存储，支持消息写入/拉取与偏移量管理 |

**模块依赖**：mq-proxy-core

### mq-proxy-sdk

独立 Java SDK，供客户端应用直接通过代理收发消息。内置重试、故障隔离（自动屏蔽失败地址）、指标采集、链路追踪与长轮询拉取。

| 关键类 | 说明 |
|--------|------|
| `ProxyClient` | SDK 主入口，提供 `send`、`pull`、`queryConsumerOffset`、`updateConsumerOffset` 等方法 |
| `ProxyClientConfig` | 配置代理地址列表、超时、重试次数、故障隔离时长、长轮询参数等 |
| `ProxyClientFacade` | 编排 SDK 内部组件，实现重试与故障隔离逻辑 |
| `ProxyAddressManager` | 代理地址轮询选择与故障隔离（失败地址临时屏蔽） |
| `ProxyChannelManager` | 管理 Netty Channel 生命周期与空闲连接清理 |
| `ProxyRemotingClient` | SDK 层 Netty 客户端，管理响应 Future 与服务端推送处理 |
| `MetricsCollector` / `ProxyMetrics` | 按代理地址统计成功/失败数、成功率与平均延迟 |
| `TraceCollector` / `TraceRecord` | 异步链路追踪（有界队列，最多 10000 条） |

**模块依赖**：mq-proxy-core

### mq-proxy-admin

管理模块（占位），计划用于 HTTP API、JMX 或管理控制台，当前暂无实际代码。

**模块依赖**：mq-proxy-core

### mq-proxy-standalone

打包启动模块，将 core、rocketmq、mock 汇总为可执行 JAR，主类为 `com.mq.proxy.core.ProxyStartup`。

**模块依赖**：mq-proxy-core、mq-proxy-rocketmq、mq-proxy-mock

### mq-proxy-test

端到端集成测试模块，使用原生 RocketMQ 客户端（不依赖任何 mq-proxy 模块）验证代理的透明性。

| 关键类 | 说明 |
|--------|------|
| `RocketMQThroughPushProxyTest` | 生产者 + Push 消费者通过代理收发消息 |
| `ConsumerTest` / `RocketMQPullConsumerTest` | Pull 消费者通过代理拉取消息 |

**模块依赖**：无（仅使用原生 rocketmq-client）

### mq-proxy-example

SDK 使用示例与基准测试，供开发者参考集成方式。

| 关键类 | 说明 |
|--------|------|
| `ProxySDKProducerQuickStart` / `ProxySDKConsumerQuickStart` | SDK 快速入门 |
| `ProducerConsumerQuickStart` | 生产者 + 消费者组合示例 |
| `ComparisonExample` | Proxy SDK 与原生 RocketMQ 客户端对比演示 |
| `SyncProducer` / `BatchProducer` / `MultiProxyProducer` | 同步发送、批量发送、多代理地址高可用示例 |
| `PullConsumer` | Pull 消费示例 |
| `BenchmarkProducer` | 性能基准测试 |

**模块依赖**：mq-proxy-sdk

## 模块依赖关系

```
mq-proxy-core          （基础层，无兄弟模块依赖）
  ├── mq-proxy-rocketmq   （存储适配器：RocketMQ）
  ├── mq-proxy-mock       （存储适配器：内存 Mock）
  ├── mq-proxy-sdk        （客户端 SDK）
  ├── mq-proxy-admin      （管理，占位）
  └── mq-proxy-standalone （打包启动 ← core + rocketmq + mock）
         mq-proxy-example  （示例 ← sdk）
         mq-proxy-test     （集成测试 ← 无代理模块依赖）
```

## 快速启动

1. 启动 NameServer 与 Broker（参考 CLAUDE.md）
2. 配置 `proxy.conf`
3. 运行 mq-proxy-standalone 生成的可执行 JAR
4. 客户端将 `namesrvAddr` 设为代理地址即可透明接入

## 技术栈

- Java 8
- Netty 4.1.68
- RocketMQ 4.9.8
- Jackson 2.12.7
- Logback 1.2.11