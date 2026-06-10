# Async Pull Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Proxy 到 Broker 的 `PULL_MESSAGE` 转发从同步阻塞改为异步回调，解除长轮询对线程数的线性占用，同时保留现有 send/offset 等同步语义。

**Architecture:** 在 `NettyRemotingClient` 增加异步请求能力，把 in-flight 请求统一放入 `responseTable`，由响应到达、超时扫描、连接断开三条路径竞争完成同一个 `ResponseFuture`。`RocketMQStorageAdapter`、`MessageEngine`、`PullMessageProcessor` 逐层新增异步 pull 接口，最终由 `PullMessageProcessor` 在回调中异步写回客户端响应。

**Tech Stack:** Java 8, Netty, JUnit4, Mockito, Maven

---

### Task 1: 规划异步请求基础设施

**Files:**
- Modify: `mq-proxy-core/src/main/java/com/mq/proxy/core/server/ResponseFuture.java`
- Modify: `mq-proxy-core/src/main/java/com/mq/proxy/core/server/NettyRemotingClient.java`
- Test: `mq-proxy-core/src/test/java/com/mq/proxy/core/server/NettyRemotingClientTest.java`

- [ ] 为 `ResponseFuture` 增加 once-only 完成保护、异步回调、超时判断
- [ ] 为 `NettyRemotingClient` 增加 `invokeAsync`、超时扫描、断链 fail-fast
- [ ] 为 `NettyRemotingClient` 增加针对以下场景的测试：
  - 响应到达时异步回调触发一次
  - channel 断开时 pending future 立即失败
  - 超时扫描时 pending future 触发超时失败

### Task 2: 打通 storage/engine 异步 pull 链路

**Files:**
- Modify: `mq-proxy-core/src/main/java/com/mq/proxy/core/storage/StorageAdapter.java`
- Create: `mq-proxy-core/src/main/java/com/mq/proxy/core/storage/PullMessageCallback.java`
- Modify: `mq-proxy-rocketmq/src/main/java/com/mq/proxy/rocketmq/adapter/RocketMQStorageAdapter.java`
- Modify: `mq-proxy-core/src/main/java/com/mq/proxy/core/engine/MessageEngine.java`
- Test: `mq-proxy-rocketmq/src/test/java/com/mq/proxy/rocketmq/adapter/RocketMQStorageAdapterTest.java`
- Test: `mq-proxy-core/src/test/java/com/mq/proxy/core/engine/MessageEngineTest.java`

- [ ] 在 `StorageAdapter` 增加 `pullMessageAsync`
- [ ] 抽取 `RocketMQStorageAdapter` 的 pull 响应解析逻辑给同步/异步共用
- [ ] 在 `MessageEngine` 增加 `pullMessageAsync`
- [ ] 保持 proxy 本地失败返回 `SYSTEM_ERROR + preserved offset`

### Task 3: PullMessageProcessor 异步响应

**Files:**
- Modify: `mq-proxy-core/src/main/java/com/mq/proxy/core/engine/processor/PullMessageProcessor.java`
- Test: `mq-proxy-core/src/test/java/com/mq/proxy/core/engine/processor/PullMessageProcessorTest.java`

- [ ] 将 `PullMessageProcessor` 改为发起异步 pull 后立刻返回 `null`
- [ ] 在异步回调中构造 `RemotingCommand` 并写回客户端
- [ ] 回调前检查客户端 channel 是否仍然可用
- [ ] 保持 `TOPIC_NOT_EXIST` / `SUBSCRIPTION_NOT_LATEST` 等现有协议映射逻辑

### Task 4: 回归验证

**Files:**
- Modify: `mq-proxy-standalone/README.md`（仅当接口或行为说明需要补充时）

- [ ] 运行 `mvn test -pl mq-proxy-core -Dtest=NettyRemotingClientTest,MessageEngineTest,PullMessageProcessorTest`
- [ ] 运行 `mvn test -pl mq-proxy-rocketmq -Dtest=RocketMQStorageAdapterTest`
- [ ] 运行 `mvn test -pl mq-proxy-core`
- [ ] 运行 `mvn test -pl mq-proxy-rocketmq`
