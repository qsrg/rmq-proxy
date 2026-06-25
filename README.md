# MQ Proxy

MQ Proxy 是一个基于 Netty 的 RocketMQ 透明代理。原生 RocketMQ 客户端只需要把 `namesrvAddr` 指向 Proxy，Proxy 会代理 NameServer 路由查询、客户端心跳、生产、消费、offset、队列锁等 RocketMQ remoting 请求，并将请求转发到真实 RocketMQ Broker 集群。

当前主干以 RocketMQ 4.9.8 原生客户端透明接入为核心目标，不包含独立 `mq-proxy-sdk` 模块。

## 架构概览

```text
RocketMQ Client
      |
      | namesrvAddr = proxy.host:proxy.listenPort
      v
MQ Proxy
  - NettyRemotingServer: 接收原生 RocketMQ remoting 请求
  - VirtualRouteManager: 查询真实 NameServer 并返回指向 Proxy 的虚拟路由
  - MessageEngine: 解析 brokerName/topic/queueId 并选择真实 Broker
  - RocketMQStorageAdapter: 通过原生 remoting 协议转发到 Broker
      |
      v
RocketMQ NameServer / Broker
```

Proxy 会监听配置端口和 RocketMQ VIP 端口（`proxy.listenPort - 2`）。例如 `proxy.listenPort=19876` 时，客户端可能连接 `19876` 或 `19874`。

## 模块介绍

### mq-proxy-core

核心模块，包含协议编解码、Netty 服务端/客户端、请求分发、路由虚拟化、客户端连接管理与存储抽象。

| 关键类 | 说明 |
|--------|------|
| `ProxyStartup` | 启动入口，加载配置并组装核心组件 |
| `ProxyConfig` / `ProxyConfigLoader` | Proxy 配置对象与加载逻辑 |
| `RemotingCommand` / `RemotingCommandEncoder` / `RemotingCommandDecoder` | RocketMQ remoting 协议帧编解码 |
| `NettyRemotingServer` | 面向 RocketMQ 客户端的 Netty 服务端 |
| `NettyRemotingClient` | Proxy 内部到 Broker 的 Netty 客户端 |
| `VirtualRouteManager` | 查询真实 NameServer 路由并生成指向 Proxy 的虚拟路由 |
| `MessageEngine` | 根据请求中的 topic、brokerName、queueId 解析真实 Broker 并调用 `StorageAdapter` |
| `ProcessorRegister` | 注册 NameServer、生产、消费、客户端管理等请求处理器 |
| `ClientConnectionManager` | 管理下游客户端连接、生产者/消费者元数据和连接失效清理 |
| `ProxyBrokerHeartbeatService` | 将下游客户端心跳汇总并转发给 Broker |
| `StorageAdapter` | 存储后端抽象接口 |

### mq-proxy-rocketmq

RocketMQ 存储适配器实现。`RocketMQStorageAdapter` 负责把 Proxy 内部请求转换为 RocketMQ remoting 请求，并转发到真实 Broker。

### mq-proxy-mock

内存 Mock 存储适配器，主要用于单元测试和本地开发验证。

### mq-proxy-admin

管理模块占位，目前仅保留包结构，尚未提供 HTTP API、JMX 或控制台能力。

### mq-proxy-standalone

可运行打包模块，生成 `mq-proxy-${version}.jar` 和分发包 `mq-proxy-${version}.tar.gz`。主类为 `com.mq.proxy.core.ProxyStartup`，当前分发包包含 `mq-proxy-core` 和 `mq-proxy-rocketmq`。

### mq-proxy-example

原生 RocketMQ 客户端示例和压测工具，展示客户端如何通过 Proxy 接入 RocketMQ。

| 示例 | 说明 |
|------|------|
| `RocketMQProducerQuickStart` | 原生生产者通过 Proxy 发送消息 |
| `RocketMQConsumerQuickStart` | 原生 PushConsumer 通过 Proxy 消费消息 |
| `OrderlyProducer` / `OrderlyConsumer` | 顺序消息生产与顺序消费示例 |
| `RocketMQProxyBenchmark` | Proxy 与直连 NameServer 的压测对比工具 |

## 模块依赖关系

```text
mq-proxy-core
  ├── mq-proxy-rocketmq
  ├── mq-proxy-mock
  └── mq-proxy-admin

mq-proxy-standalone
  ├── mq-proxy-core
  └── mq-proxy-rocketmq

mq-proxy-example
  └── org.apache.rocketmq:rocketmq-client
```

## 快速启动

1. 启动本机 RocketMQ NameServer 和 Broker，命令见 `AGENTS.md`。
2. 打包 Proxy：

   ```bash
   mvn package -pl mq-proxy-standalone -am -DskipTests
   ```

3. 解压分发包并修改 `conf/proxy.properties`：

   ```properties
   proxy.listenPort=19876
   proxy.host=127.0.0.1
   proxy.namesrvAddr=127.0.0.1:9876
   ```

4. 启动 Proxy：

   ```bash
   sh bin/proxy.sh
   ```

5. 原生 RocketMQ 客户端将 `namesrvAddr` 配置为 Proxy 地址：

   ```java
   producer.setNamesrvAddr("127.0.0.1:19876");
   consumer.setNamesrvAddr("127.0.0.1:19876");
   ```

## 常用文档

- Standalone 打包、配置、TLS 与部署说明：[mq-proxy-standalone/README.md](mq-proxy-standalone/README.md)
- 原生客户端示例与压测说明：[mq-proxy-example/README.md](mq-proxy-example/README.md)
- 当前 docs 索引：[docs/README.md](docs/README.md)

## 技术栈

- Java 8
- Netty 4.1.68
- RocketMQ 4.9.8
- Jackson 2.12.7
- SLF4J 1.7.36
- Logback 1.2.11
