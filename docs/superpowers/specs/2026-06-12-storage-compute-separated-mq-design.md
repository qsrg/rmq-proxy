# 存储计算分离 MQ 设计文档

## 1. 背景

当前 mq-proxy 已经可以作为 RocketMQ 协议代理存在，客户端仍主要沿用 RocketMQ 原生客户端和 Broker/NameServer 模型。后续如果底层 broker 自研，继续兼容 RocketMQ broker 协议会把大量 broker 计算逻辑留在客户端和后端协议里，客户端仍然偏重，proxy 也难以成为真正的计算层。

新的方向是构建一套存储计算分离的 MQ 架构：

- 客户端使用新的轻量 SDK，不再使用 RocketMQ 原生客户端。
- proxy 承担 MQ 的计算层职责，包括路由、消费组协调、流控、ack、retry、DLQ、offset 推进。
- 底层自研存储引擎只提供持久化、顺序追加、按 offset 读取、元数据和 offset 存储能力。
- RocketMQ 协议兼容可以继续作为迁移入口存在，但不是新架构的核心路径。

## 2. 目标

1. 提供轻量 Java SDK，应用侧只感知 send、subscribe、ack、nack 等语义。
2. 使用 gRPC 作为第一版客户端到 proxy 的通信协议。
3. proxy 作为计算层，集中管理 topic 路由、consumer group、partition 分配、流控和消费进度。
4. 存储层与计算层解耦，存储接口只暴露 append/fetch/metadata/offset 等基础能力。
5. 第一版支持普通消息、批量发送、callback 消费、ack/nack、at-least-once、简化 retry/DLQ。
6. 为后续顺序消费预留 key、partition、per-partition 投递控制能力。

## 3. 非目标

第一版不实现以下能力：

1. RocketMQ 原生客户端兼容的新链路。
2. 事务消息。
3. 完整延迟消息和时间轮。
4. exactly-once 语义。
5. 跨地域复制。
6. 复杂 tag/sql 过滤。
7. 多租户权限体系。
8. 完整控制台和运维平台。

这些能力可以在主链路稳定后分阶段补充。

### 3.1 当前文档可开发性结论

第一版设计可以进入开发，但需要把落地边界控制在单机 MVP：

- proxy 单节点运行，先不解决跨 proxy 的强一致 membership。
- storage 使用本地文件型 `LocalStorageEngine`，接口按未来分布式存储设计。
- metadata、offset、retry 先持久化到本地 store 目录，格式保持可替换。
- SDK 只做 Java 版本。
- 消费只提供 concurrent callback，顺序消费只预留模型。

如果第一阶段直接做多 proxy、分布式存储副本、顺序消费和复杂 retry，范围会失控。正确路径是先把协议、SDK、proxy compute 和 storage API 定下来，保证后续替换存储或扩展多 proxy 时不改 SDK 主 API。

## 4. 总体架构

```
┌──────────────────────────────────────────────────────────────┐
│                         应用层                                │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                    Light MQ SDK                         │  │
│  │  send / batchSend / subscribe / ack / nack / shutdown  │  │
│  └──────────────────────────┬─────────────────────────────┘  │
└─────────────────────────────┼────────────────────────────────┘
                              │ gRPC
┌─────────────────────────────▼────────────────────────────────┐
│                      Proxy 计算层                             │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ gRPC Gateway                                           │  │
│  │ ProducerComputeService                                 │  │
│  │ ConsumerCoordinator                                    │  │
│  │ PartitionAssigner                                      │  │
│  │ DeliveryService                                        │  │
│  │ InflightTracker                                        │  │
│  │ OffsetService                                          │  │
│  │ RetryService / DLQService                              │  │
│  │ MetadataService                                        │  │
│  └──────────────────────────┬─────────────────────────────┘  │
└─────────────────────────────┼────────────────────────────────┘
                              │ Storage API
┌─────────────────────────────▼────────────────────────────────┐
│                      自研存储层                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Topic Metadata                                         │  │
│  │ Partition Log                                          │  │
│  │ Consumer Offset                                        │  │
│  │ Retry/DLQ Records                                      │  │
│  └────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

核心原则：

- SDK 轻：不暴露 brokerName、queueId、offset、rebalance 等内部概念。
- proxy 重：所有 MQ 计算逻辑集中在 proxy。
- storage 薄：只做持久化和基础索引，不感知客户端连接和消费组调度。

## 5. SDK 设计

### 5.1 用户 API

主消费 API 使用 callback 风格：

```java
MqClient client = MqClient.builder()
        .proxyAddress("127.0.0.1:10914")
        .build();

Producer producer = client.producer();
SendResult sendResult = producer.send("TopicA", "order-10001", body);

Consumer consumer = client.consumer("GroupA")
        .concurrency(8)
        .maxInflight(256)
        .ackTimeout(Duration.ofSeconds(30))
        .subscribe("TopicA", message -> {
            try {
                handle(message);
                return ConsumeResult.SUCCESS;
            } catch (Exception e) {
                return ConsumeResult.RETRY;
            }
        });
```

主 API 使用返回值表达消费结果，由 SDK 统一发送 ack/nack，避免用户忘记 ack 或重复调用 ack：

```java
enum ConsumeResult {
    SUCCESS,
    RETRY,
    DROP
}
```

`ConsumeResult` 语义：

| 结果 | SDK 行为 | Proxy 行为 |
| --- | --- | --- |
| SUCCESS | 发送 ack | 标记当前投递成功，尝试推进 committed offset |
| RETRY | 发送 nack | 进入 retry，超过最大次数后进入 DLQ |
| DROP | 发送 nack，action=DROP | 不再重试，写入 DLQ 记录，并把原 offset 标记为完成 |

`DROP` 不是静默丢弃。MVP 中必须写 DLQ 记录，便于排查；后续可以通过配置允许直接丢弃。

手动 ack/nack 保留为高级模式，只给需要异步处理消息的场景使用：

```java
consumer.subscribeManual("TopicA", message -> {
    asyncHandle(message).whenComplete((ignored, error) -> {
        if (error == null) {
            message.ack();
        } else {
            message.nack();
        }
    });
});
```

辅助 API 可以提供 poll，用于测试、批处理和命令行工具：

```java
List<Message> messages = consumer.poll("TopicA", 32, Duration.ofSeconds(3));
```

主路径仍以 callback 为准。

### 5.2 SDK 职责

SDK 负责：

1. gRPC channel 复用。
2. consumer stream 建立和重连。
3. listener 线程池。
4. maxInflight 本地限制。
5. credit 流控上报。
6. ack/nack 异步发送。
7. shutdown 时等待本地处理中消息完成。

SDK 不负责：

1. topic 路由。
2. partition 分配。
3. consumer rebalance。
4. offset 管理。
5. retry/DLQ 调度。
6. broker 故障感知。

### 5.2.1 SDK 复杂度预算

轻 SDK 不代表没有状态。第一版必须限制 SDK 内部复杂度，避免重新长成一个重客户端：

1. 一个 `MqClient` 进程内共享一个 gRPC channel pool。
2. 每个 consumer group/topic subscription 对应一个 consume stream。
3. 每个 consume stream 维护一个本地 inflight map，大小不超过 `maxInflight`。
4. callback 执行线程池由 `concurrency` 控制，默认 8。
5. ack/nack 发送失败时在 stream 存活期间重试；stream 断开后，不再本地持久化 ack，未确认消息由 proxy 超时重投。
6. reconnect 后 SDK 重新 subscribe，不携带本地 offset。
7. SDK 不缓存路由、不缓存 partition assignment、不做 rebalance 计算。
8. SDK shutdown 默认等待本地 callback 完成，超过 `shutdownTimeout` 后关闭 stream，未 ack 消息由 proxy 重投。

这些约束保证 SDK 只是协议和回调执行器，不承担 broker 计算逻辑。

### 5.3 轻量边界

SDK 暴露给用户的 Message 只包含业务语义：

```java
class Message {
    String messageId;
    String topic;
    String key;
    Map<String, String> properties;
    byte[] body;
    int deliveryAttempt;
}
```

默认 callback 模式下，`Message` 不暴露 `ack()` 和 `nack()`。`ackToken` 只在 SDK 内部保存，不暴露给应用代码。只有 `subscribeManual` 返回的 `ManualMessage` 才暴露 `ack()` 和 `nack()`。

## 6. gRPC 协议设计

### 6.1 服务定义

第一版划分为三个服务：

```protobuf
service MessageService {
  rpc Send(SendRequest) returns (SendResponse);
  rpc BatchSend(BatchSendRequest) returns (BatchSendResponse);
}

service ConsumerService {
  rpc Consume(stream ClientFrame) returns (stream ServerFrame);
}

service AdminService {
  rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
  rpc DescribeTopic(DescribeTopicRequest) returns (DescribeTopicResponse);
}
```

消费使用双向流。一个 consumer session 的订阅、投递、ack、nack、心跳和流控都放在同一个 stream 中，便于 proxy 维护会话状态。

### 6.2 ClientFrame

```protobuf
message ClientFrame {
  string request_id = 1;
  oneof payload {
    SubscribeRequest subscribe = 10;
    AckRequest ack = 11;
    NackRequest nack = 12;
    FlowCredit flow_credit = 13;
    Heartbeat heartbeat = 14;
    CloseRequest close = 15;
  }
}
```

语义：

| Frame | 说明 |
| --- | --- |
| SubscribeRequest | 建立订阅，携带 group、topics、clientId、maxInflight |
| AckRequest | 确认消息处理成功 |
| NackRequest | 声明消息处理失败 |
| FlowCredit | SDK 上报还可接收多少条消息或多少字节 |
| Heartbeat | 保活和状态上报 |
| CloseRequest | 优雅关闭 stream |

### 6.3 ServerFrame

```protobuf
message ServerFrame {
  string request_id = 1;
  oneof payload {
    SubscribeResponse subscribed = 10;
    DeliverRequest deliver = 11;
    FlowControl flow_control = 12;
    Heartbeat heartbeat = 13;
    ErrorResponse error = 14;
  }
}
```

语义：

| Frame | 说明 |
| --- | --- |
| SubscribeResponse | 订阅成功，返回 sessionId 和初始分配信息 |
| DeliverRequest | 投递一批消息 |
| FlowControl | proxy 要求 SDK 降低 credit 或暂停消费 |
| Heartbeat | proxy 保活 |
| ErrorResponse | 协议错误、鉴权失败、topic 不存在等 |

### 6.4 消息投递结构

```protobuf
message DeliveredMessage {
  string message_id = 1;
  string topic = 2;
  string key = 3;
  map<string, string> properties = 4;
  bytes body = 5;
  int32 delivery_attempt = 6;
  string ack_token = 7;
}
```

`ack_token` 用于 proxy 识别投递记录。SDK 只回传 token，不解析 token 内容。

### 6.5 完整 proto 草案

第一版 proto 先放在 `mq-proxy-grpc/src/main/proto/mq_proxy.proto`。包名建议：

```protobuf
syntax = "proto3";

package mq.proxy.v1;

option java_package = "com.mq.proxy.grpc.v1";
option java_multiple_files = true;
option java_outer_classname = "MqProxyProto";

service MessageService {
  rpc Send(SendRequest) returns (SendResponse);
  rpc BatchSend(BatchSendRequest) returns (BatchSendResponse);
}

service ConsumerService {
  rpc Consume(stream ClientFrame) returns (stream ServerFrame);
}

service AdminService {
  rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse);
  rpc DescribeTopic(DescribeTopicRequest) returns (DescribeTopicResponse);
}

message SendRequest {
  string request_id = 1;
  string producer_id = 2;
  string topic = 3;
  string key = 4;
  map<string, string> properties = 5;
  bytes body = 6;
  int64 born_timestamp = 7;
}

message SendResponse {
  ErrorCode code = 1;
  string message = 2;
  string message_id = 3;
  string topic = 4;
  int32 partition_id = 5;
  int64 offset = 6;
}

message BatchSendRequest {
  string request_id = 1;
  string producer_id = 2;
  repeated SendRequest messages = 3;
}

message BatchSendResponse {
  ErrorCode code = 1;
  string message = 2;
  repeated SendResponse results = 3;
}

message ClientFrame {
  string request_id = 1;
  oneof payload {
    SubscribeRequest subscribe = 10;
    AckRequest ack = 11;
    NackRequest nack = 12;
    FlowCredit flow_credit = 13;
    Heartbeat heartbeat = 14;
    CloseRequest close = 15;
  }
}

message ServerFrame {
  string request_id = 1;
  oneof payload {
    SubscribeResponse subscribed = 10;
    DeliverRequest deliver = 11;
    FlowControl flow_control = 12;
    Heartbeat heartbeat = 13;
    ErrorResponse error = 14;
  }
}

message SubscribeRequest {
  string client_id = 1;
  string group = 2;
  repeated string topics = 3;
  int32 max_inflight = 4;
  int32 concurrency = 5;
  int64 ack_timeout_millis = 6;
}

message SubscribeResponse {
  ErrorCode code = 1;
  string message = 2;
  string session_id = 3;
  int64 assignment_version = 4;
}

message DeliverRequest {
  string session_id = 1;
  repeated DeliveredMessage messages = 2;
}

message DeliveredMessage {
  string message_id = 1;
  string topic = 2;
  string key = 3;
  map<string, string> properties = 4;
  bytes body = 5;
  int32 delivery_attempt = 6;
  string ack_token = 7;
}

message AckRequest {
  string session_id = 1;
  repeated string ack_tokens = 2;
}

message NackRequest {
  string session_id = 1;
  repeated string ack_tokens = 2;
  NegativeAckAction action = 3;
  string reason = 4;
}

enum NegativeAckAction {
  RETRY = 0;
  DROP = 1;
}

message FlowCredit {
  string session_id = 1;
  int32 message_credit = 2;
}

message FlowControl {
  string session_id = 1;
  bool paused = 2;
  int32 suggested_max_inflight = 3;
  string reason = 4;
}

message Heartbeat {
  string session_id = 1;
  int64 timestamp = 2;
}

message CloseRequest {
  string session_id = 1;
  string reason = 2;
}

message ErrorResponse {
  ErrorCode code = 1;
  string message = 2;
  bool retriable = 3;
}

message CreateTopicRequest {
  string topic = 1;
  int32 partition_count = 2;
}

message CreateTopicResponse {
  ErrorCode code = 1;
  string message = 2;
}

message DescribeTopicRequest {
  string topic = 1;
}

message DescribeTopicResponse {
  ErrorCode code = 1;
  string message = 2;
  TopicMetadata topic_metadata = 3;
}

message TopicMetadata {
  string topic = 1;
  int32 partition_count = 2;
  int64 create_timestamp = 3;
}

enum ErrorCode {
  OK = 0;
  INVALID_REQUEST = 1;
  TOPIC_NOT_FOUND = 2;
  TOPIC_ALREADY_EXISTS = 3;
  STORAGE_ERROR = 4;
  TIMEOUT = 5;
  FLOW_CONTROLLED = 6;
  SESSION_NOT_FOUND = 7;
  TOKEN_EXPIRED = 8;
  TOKEN_INVALID = 9;
  INTERNAL_ERROR = 10;
}
```

### 6.6 协议兼容策略

proto 第一版发布后必须遵守：

1. 字段只新增，不复用已删除字段编号。
2. `ErrorCode` 只追加，不改变既有枚举值。
3. `DeliveredMessage` 不暴露 partition 和 offset 给用户 API，但可以在内部扩展字段中保留，便于 SDK debug 日志。
4. `request_id` 是客户端请求幂等键的一部分，Send 重试必须复用同一个 request_id。
5. 大 body 第一版直接走 gRPC bytes；超过 `proxy.grpcMaxMessageBytes` 返回 `INVALID_REQUEST`，后续再设计分片。

## 7. Proxy 计算层设计

### 7.1 模块职责

| 模块 | 职责 |
| --- | --- |
| GrpcGateway | 接入 gRPC 请求，完成协议校验和基础限流 |
| ProducerComputeService | 校验 topic，选择 partition，写入存储 |
| ConsumerCoordinator | 管理 consumer group、成员、partition 分配 |
| PartitionAssigner | 根据 group 成员和 topic partition 生成分配结果 |
| DeliveryService | 从存储读取消息并按 credit 投递 |
| InflightTracker | 跟踪已投递未 ack 消息 |
| OffsetService | 管理 committed offset 和推进规则 |
| RetryService | 处理 nack 和 ack timeout |
| DLQService | 超过最大重试次数后写入死信 |
| MetadataService | 管理 topic、partition、group 配置 |

### 7.1.1 建议模块和包结构

第一版建议新增模块，而不是把新链路直接塞进现有 RocketMQ processor：

```
mq-proxy-grpc
  src/main/proto/mq_proxy.proto
  com.mq.proxy.grpc.server
  com.mq.proxy.grpc.mapper

mq-proxy-compute
  com.mq.proxy.compute.producer
  com.mq.proxy.compute.consumer
  com.mq.proxy.compute.delivery
  com.mq.proxy.compute.offset
  com.mq.proxy.compute.retry
  com.mq.proxy.compute.metadata

mq-proxy-storage-api
  com.mq.proxy.storage.api
  com.mq.proxy.storage.api.model

mq-proxy-storage-local
  com.mq.proxy.storage.local

mq-proxy-sdk-v2
  com.mq.proxy.sdk.client
  com.mq.proxy.sdk.producer
  com.mq.proxy.sdk.consumer
  com.mq.proxy.sdk.grpc
```

`mq-proxy-standalone` 后续负责按配置启动 RocketMQ 兼容链路或 gRPC 新链路。

### 7.1.2 核心类边界

| 类 | 所属模块 | 核心方法 |
| --- | --- | --- |
| GrpcMessageService | mq-proxy-grpc | send, batchSend |
| GrpcConsumerService | mq-proxy-grpc | consume |
| ProducerComputeService | mq-proxy-compute | send, batchSend |
| ConsumerCoordinator | mq-proxy-compute | joinGroup, leaveGroup, refreshLease |
| DeliveryService | mq-proxy-compute | startDelivery, stopDelivery, onCredit |
| InflightTracker | mq-proxy-compute | add, ack, nack, expire |
| OffsetService | mq-proxy-compute | markAcked, advanceCommittedOffset |
| RetryService | mq-proxy-compute | scheduleRetry, pollDueRetries |
| StorageEngine | mq-proxy-storage-api | append, fetch, commitConsumerOffset |
| LocalStorageEngine | mq-proxy-storage-local | StorageEngine 本地文件实现 |

gRPC endpoint 只负责协议转换，不直接处理 offset、retry 和 storage 细节。

### 7.2 Consumer Session 状态

每个 stream 对应一个 ConsumerSession：

```java
class ConsumerSession {
    String sessionId;
    String clientId;
    String group;
    Set<String> topics;
    Set<PartitionRef> assignedPartitions;
    int maxInflight;
    int availableCredit;
    Map<String, InflightMessage> inflightMessages;
    long lastHeartbeatTime;
}
```

session 状态可以保存在当前 proxy 内存中；可恢复状态必须写入存储或 metadata：

- consumer group membership lease
- partition assignment version
- committed offset
- retry records

### 7.3 订阅流程

```
SDK -> Proxy: Consume stream open
SDK -> Proxy: Subscribe(group, topics, clientId, maxInflight)
Proxy: 校验 topic 和 group
Proxy: 注册 consumer membership lease
Proxy: 触发 group partition assignment
Proxy -> SDK: SubscribeResponse(sessionId)
SDK -> Proxy: FlowCredit
Proxy: 开始投递消息
```

### 7.4 投递流程

```
DeliveryService 查询 session credit
DeliveryService 按 assigned partition 读取 storage
InflightTracker 生成 ackToken 并记录 inflight
Proxy -> SDK: DeliverRequest
SDK listener 执行业务逻辑
SDK -> Proxy: AckRequest 或 NackRequest
Proxy 更新 inflight、offset 或 retry
```

### 7.5 Credit 流控

callback API 不能做无上限 push。第一版采用 credit-based flow control：

1. SDK 初始化时根据 `maxInflight` 发送初始 credit。
2. SDK 每完成一条 ack/nack，本地释放 inflight。
3. SDK 按批次发送新增 credit。
4. proxy 只在 `availableCredit > 0` 时投递。
5. proxy 可以通过 FlowControl 要求 SDK 暂停或降低 credit。

credit 可以同时按条数和字节控制，第一版先实现条数控制，字节控制作为后续增强。

### 7.6 Send 幂等

SDK 发送重试会带来重复写入风险。第一版必须定义幂等键：

```text
producerId + requestId
```

proxy 在 `ProducerComputeService` 中先查询短期去重缓存：

1. 命中：直接返回原 SendResponse。
2. 未命中：选择 partition，append storage，记录去重结果。

单机 MVP 中去重缓存可以使用内存 LRU + 本地快照，保留时间默认 10 分钟。proxy 重启后允许少量发送重复，但不能丢消息。后续生产版本需要把 send dedup 写入 metadata/storage。

BatchSend 的幂等粒度：

- batch 请求整体用 `BatchSendRequest.request_id`。
- 单条消息仍有自己的 `SendRequest.request_id`。
- 重试 batch 时应复用所有 request_id。

### 7.7 后台调度线程

proxy compute 第一版至少需要三个后台任务：

| 任务 | 周期 | 职责 |
| --- | --- | --- |
| LeaseScanner | 1s | 扫描 consumer session lease，过期后触发 rebalance |
| AckTimeoutScanner | 1s | 扫描 inflight 超时消息，转 retry |
| RetryDispatcher | 1s | 扫描到期 retry 记录，重新投递或写 DLQ |

这些任务必须可关闭，standalone shutdown 时按顺序停止：停止接收新请求、停止投递、等待短时间 ack、刷盘、关闭 storage。

## 8. Offset 与 Ack 语义

### 8.1 投递语义

第一版提供 at-least-once：

- 消息可能重复投递。
- ack 成功后才推进 committed offset。
- proxy 或 SDK 重启后，从 committed offset 继续投递。
- 应用需要按 messageId 或业务 key 做幂等。

### 8.2 AckToken

ackToken 应包含以下信息：

```text
sessionId
group
topic
partitionId
offset
deliveryAttempt
expireTime
signature
```

签名由 proxy 生成，用于避免 SDK 伪造 token。proxy 收到 ack/nack 后校验：

1. token 签名有效。
2. token 未过期。
3. token 属于当前有效 session 或可接受的历史 session。
4. token 对应的 inflight 消息仍未完成。

重复 ack 返回幂等成功，不应导致 offset 错误推进。

### 8.3 并发消费下的 offset 推进

并发消费时不能简单按 ack 的最大 offset 提交。每个 `group + topic + partition` 维护：

```text
committedOffset
ackedOffsetSet
inflightOffsetSet
```

规则：

1. deliver offset N 后加入 inflight。
2. ack offset N 后从 inflight 移除，加入 acked。
3. 从 committedOffset 开始连续检查 acked。
4. 只有连续 ack 的 offset 才能推进 committedOffset。
5. 如果 offset 10 ack，offset 9 未 ack，committedOffset 不能越过 9。

这样可以支持并发 callback，同时保持 partition offset 的正确推进。

### 8.4 Nack 与超时

nack 或 ack timeout 后：

1. 从 inflight 移除。
2. deliveryAttempt 加一。
3. 未超过最大重试次数时写入 retry。
4. 超过最大重试次数时写入 DLQ。
5. 原 partition 的 committedOffset 仍遵守连续 ack 规则。

第一版 retry 可以采用简单延迟队列或固定延迟扫描，不要求完整时间轮。

### 8.4.1 Retry/DLQ 与 Offset 推进

retry 不能让一个毒丸消息永久阻塞 committed offset。对每个原始消息 offset，最终必须进入以下终态之一：

```text
ACKED
DLQED
DROPPED
```

OffsetService 只把这些终态视为“完成”，并按连续完成区间推进 committed offset：

1. 消息正常消费成功：标记 `ACKED`。
2. 消息 nack 或 ack timeout：进入 retry，原 offset 暂不完成。
3. retry 消费成功：标记原 offset 为 `ACKED`。
4. retry 超过最大次数：写 DLQ，标记原 offset 为 `DLQED`。
5. 用户返回 `ConsumeResult.DROP`：写 DLQ/drop 记录，标记原 offset 为 `DROPPED`。

因此，DLQ 不是 offset 推进的旁路，而是原 offset 的一个完成状态。否则 offset 9 成为毒丸消息时，offset 10、11 即使已经 ack，也无法在重启后避免重复。

MVP 中 completion state 可以保存在 OffsetService 内存中；committed offset 推进后持久化即可。proxy 异常退出时，未推进的 completion state 丢失会导致重复消费，但不会丢消息。

### 8.5 AckToken 编码

第一版 ackToken 使用 URL-safe Base64 编码，内容为 JSON 或紧凑二进制均可。为了便于调试，MVP 使用 JSON 后再签名：

```json
{
  "sid": "session-id",
  "grp": "GroupA",
  "t": "TopicA",
  "p": 0,
  "o": 123,
  "a": 1,
  "exp": 1710000000000,
  "sig": "hmac-sha256"
}
```

签名输入不包含 `sig` 字段。密钥来自 proxy 配置：

```properties
proxy.ackTokenSecret=local-development-secret
```

MVP 可以在启动时如果未配置则生成随机 secret，但重启后旧 token 全部失效，导致未 ack 消息走超时重投。这符合 at-least-once。

### 8.6 Offset 持久化时机

OffsetService 每次推进 committedOffset 后立即调用 storage 持久化。为了减少刷盘频率，本地存储可以合并写，但必须满足：

1. ack 返回给 SDK 前，offset 推进结果已经进入内存状态。
2. proxy 正常关闭时必须 flush offset。
3. proxy 异常退出允许重复消费最近已 ack 但未 flush 的消息。
4. 不允许把未连续 ack 的 offset 提交到持久化层。

## 9. 存储层设计

### 9.1 存储接口

存储层只暴露基础日志和元数据能力：

```java
interface StorageEngine {
    AppendResult append(AppendRequest request);

    FetchResult fetch(FetchRequest request);

    OffsetRange getOffsetRange(String topic, int partitionId);

    void commitConsumerOffset(OffsetCommit commit);

    long queryConsumerOffset(String group, String topic, int partitionId);

    TopicMetadata getTopic(String topic);

    void createTopic(CreateTopicRequest request);
}
```

完整请求和结果模型建议：

```java
class AppendRequest {
    String topic;
    int partitionId;
    MessageRecord record;
}

class AppendResult {
    String messageId;
    int partitionId;
    long offset;
    long storeTimestamp;
}

class FetchRequest {
    String topic;
    int partitionId;
    long offset;
    int maxMessages;
    int maxBytes;
}

class FetchResult {
    List<MessageRecord> records;
    long nextOffset;
    long minOffset;
    long maxOffset;
}

class OffsetCommit {
    String group;
    String topic;
    int partitionId;
    long committedOffset;
}
```

### 9.2 Topic 与 Partition 模型

```
TopicA
  Partition 0: offset 0, 1, 2, 3 ...
  Partition 1: offset 0, 1, 2, 3 ...
  Partition 2: offset 0, 1, 2, 3 ...
```

每条消息记录包含：

```text
messageId
topic
partitionId
offset
key
properties
body
bornTimestamp
storeTimestamp
producerId
sequenceId
crc
```

`key`、`partitionId`、`offset` 第一版就必须写入，即使暂时不开放顺序消费。

### 9.3 Partition 选择

partition 选择必须接口化：

```java
interface PartitionSelector {
    int select(String topic, String key, int partitionCount);
}
```

第一版普通消息默认策略：

- 有 key：使用稳定 hash，`hash(key) % partitionCount`。
- 无 key：round-robin。

为了后续顺序消费更平滑，推荐第一版对有 key 的消息就使用 `hash(key) % partitionCount`。这样后续开启 key 顺序时，不需要改变已有 key 的分区稳定性。

### 9.4 存储层不承担的职责

存储层不处理：

1. consumer stream。
2. SDK 连接。
3. listener 并发。
4. group rebalance。
5. credit 流控。
6. ack timeout。
7. retry 调度策略。

这些都属于 proxy 计算层。

### 9.5 LocalStorageEngine 定位

`LocalStorageEngine` 是第一阶段单机 MVP 的本地文件存储实现。它不是最终分布式存储，但必须满足两个要求：

1. API 和数据模型对齐未来分布式 `StorageEngine`，后续替换存储时不改 SDK 和 proxy compute 主流程。
2. 本地实现本身可恢复、可测试，能支撑 send、fetch、offset、retry、DLQ 的完整闭环。

MVP 中不做：

- 多副本。
- 文件清理和保留策略。
- mmap 优化。
- 跨进程并发写同一个 store 目录。
- 消息压缩和加密。

这些作为后续生产化能力补充。

### 9.6 LocalStorageEngine 组件

```text
LocalStorageEngine
  MessageStore
    PhysicalLogManager
      PhysicalLogShard
      PhysicalLogSegment
    LogicalQueueIndexManager
      LogicalQueueIndex
  LocalMetadataStore
    TopicMetadataStore
    ConsumerOffsetStore
  LocalDeliveryStateStore
    RetryStore
    DlqStore
  StoreCheckpoint
```

| 组件 | 职责 |
| --- | --- |
| LocalStorageEngine | 实现 StorageEngine，协调各子组件启动、关闭、恢复和刷盘 |
| MessageStore | 存储消息本体和逻辑队列索引，是未来分布式存储替换的核心接口 |
| TopicMetadataStore | 管理 topic、partitionCount、创建时间 |
| PhysicalLogManager | 管理固定数量的共享物理追加日志 shard |
| PhysicalLogShard | 顺序追加消息数据，避免每个 topic/partition 一个物理数据文件 |
| LogicalQueueIndexManager | 管理 topic/partition 到物理位置的逻辑索引 |
| LogicalQueueIndex | 维护逻辑 offset 到 physical shard/segment/position 的映射 |
| LocalMetadataStore | MVP 本地元数据持久化，包含 topic 和 consumer offset |
| ConsumerOffsetStore | 持久化 group/topic/partition 的 committed offset，属于 metadata 状态 |
| LocalDeliveryStateStore | MVP 本地投递状态持久化，包含 retry 和 DLQ |
| RetryStore | 保存待重试消息引用，属于 proxy compute 的持久化状态 |
| DlqStore | 保存死信消息，属于 proxy compute 的持久化状态 |
| StoreCheckpoint | 保存最近一次正常 flush 的时间和位置 |

第一版可以把所有组件放在 `mq-proxy-storage-local` 模块中，但包结构要拆清楚：

```text
com.mq.proxy.storage.local
com.mq.proxy.storage.local.metadata
com.mq.proxy.storage.local.physical
com.mq.proxy.storage.local.index
com.mq.proxy.storage.local.offset
com.mq.proxy.storage.local.retry
com.mq.proxy.storage.local.recovery
```

职责边界：

1. `MessageStore` 是真正的消息存储引擎，后续替换为分布式存储时优先替换它。
2. `LocalMetadataStore` 和 `LocalDeliveryStateStore` 是单机 MVP 的本地持久化实现，服务于 proxy compute；后续多 proxy 阶段应迁移到独立 metadata store。
3. `StorageEngine` 对外暂时聚合这些能力，是为了 MVP 接口简单，不代表长期把 MQ 计算状态放进消息存储层。

### 9.6.1 核心类接口草案

`LocalStorageEngine` 的实现入口：

```java
class LocalStorageEngine implements StorageEngine {
    void start(LocalStoreConfig config);
    void shutdown();
    void flush();

    AppendResult append(AppendRequest request);
    FetchResult fetch(FetchRequest request);
    OffsetRange getOffsetRange(String topic, int partitionId);
    void commitConsumerOffset(OffsetCommit commit);
    long queryConsumerOffset(String group, String topic, int partitionId);
    TopicMetadata getTopic(String topic);
    void createTopic(CreateTopicRequest request);
}
```

核心内部接口：

```java
class PhysicalLogManager {
    PhysicalAppendResult append(MessageRecord record);
    MessageRecord read(PhysicalPosition position, int recordLength);
    void recover();
    void flush();
}

class PhysicalLogShard {
    PhysicalAppendResult append(ByteBuffer recordBuffer);
    MessageRecord read(PhysicalPosition position, int recordLength);
    void rollSegmentIfNeeded(int appendBytes);
}

class LogicalQueueIndexManager {
    LogicalQueueIndex getOrCreate(String topic, int partitionId);
    LogicalQueueIndex getIfExists(String topic, int partitionId);
    void rebuildFromPhysicalLogs(PhysicalLogScanner scanner);
}

class LogicalQueueIndex {
    long nextOffset();
    void append(IndexEntry entry);
    List<IndexEntry> read(long offset, int maxEntries);
    OffsetRange offsetRange();
}
```

物理位置模型：

```java
class PhysicalPosition {
    int shardId;
    long segmentBaseOffset;
    long position;
}

class PhysicalAppendResult {
    PhysicalPosition position;
    int recordLength;
    long storeTimestamp;
}

class IndexEntry {
    long logicalOffset;
    PhysicalPosition physicalPosition;
    int recordLength;
    long storeTimestamp;
}
```

`PhysicalPosition.position` 是 segment 文件内偏移，不是全局物理偏移。`segmentBaseOffset + position` 可以作为 shard 内绝对物理偏移，用于 checkpoint 和诊断。

### 9.6.2 内存状态

启动后必须维护以下内存状态：

```java
class LocalStoreRuntime {
    Map<String, TopicMetadata> topics;
    Map<LogicalQueueKey, LogicalQueueState> logicalQueues;
    PhysicalShardState[] physicalShards;
    OpenFileCache openFileCache;
}

class LogicalQueueState {
    String topic;
    int partitionId;
    long minOffset;
    long maxOffset;
    LogicalQueueStatus status;
    String fencedReason;
    ReentrantLock offsetLock;
}

enum LogicalQueueStatus {
    OPEN,
    FENCED
}

class PhysicalShardState {
    int shardId;
    long activeSegmentBaseOffset;
    long activeSegmentWrotePosition;
    ReentrantLock appendLock;
}
```

`maxOffset` 是 exclusive 语义。append 只有在 physical log 和 logical index 都写成功后才能推进内存 `maxOffset`。

`FENCED` 表示该 logical queue 的最后一次 append 结果不确定。进入该状态后，只允许 fetch 已经可见的 offset，不允许继续 append。重启恢复成功后状态回到 `OPEN`。

### 9.6.3 文件句柄缓存

logical index 文件数量会随实际写入的 topic partition 增长，不能全部常驻打开。第一版使用 LRU 文件句柄缓存：

```java
class OpenFileCache {
    SeekableByteChannel acquire(Path path, OpenOption... options);
    void release(Path path);
    void evictIdleFiles();
    void closeAll();
}
```

规则：

1. `proxy.localStoreMaxOpenIndexFiles` 控制同时打开的 logical index 文件数。
2. physical active segment 常驻打开；historical segment 按读取需要通过缓存打开。
3. 被 LRU 淘汰的文件必须先 flush 再 close。
4. fetch 读取历史 segment 时，不应长期占用文件句柄。
5. shutdown 必须关闭所有缓存文件。

### 9.7 Topic 多时的性能问题与结论

如果采用 Kafka 风格的 `topic/partition -> 独立 log 文件`，topic 很多时会有明显问题：

1. 文件数量膨胀：文件数约等于 `topicCount * partitionCount * segmentCount * 2`，还要乘上 index 文件。
2. 文件句柄压力：活跃 topic 多时需要频繁打开/关闭文件，或者占用大量 fd。
3. 恢复变慢：启动时要扫描大量小文件和目录。
4. 刷盘分散：大量小文件 force 会造成随机 IO 和调度开销。
5. 小 topic 浪费：低流量 topic 也会持有独立数据文件。

因此 LocalStorageEngine 不把 `topic/partition` 直接设计成物理 log 文件。正确边界是：

```text
逻辑模型: topic + partition + offset
物理模型: 固定数量 physical log shard + logical queue index
```

SDK 和 compute 层仍然使用 topic partition offset；LocalStorageEngine 内部把消息 body 追加到少量共享物理日志，再用逻辑队列索引定位。

MVP 对 topic 数量的处理策略：

1. physical log shard 数固定，不随 topic 增长。
2. logical index 文件惰性创建，只有 partition 第一次写入时才创建。
3. logical index 文件句柄使用 LRU 缓存，避免 topic 多时长期占用大量 fd。
4. 启动恢复先扫描 physical log，再按需要重建 logical index；后续可用 checkpoint 缩短扫描范围。
5. 如果 topic 数量达到十万级，per-partition index 文件也会成为问题，届时应升级为 index shard 或 RocksDB/LSM index。

MVP 明确容量边界：

| 维度 | MVP 建议上限 | 超过后的升级方向 |
| --- | --- | --- |
| topic 数 | 1000 | metadata 分片、topic 管理服务 |
| logical partition 数 | 16000 | index shard 或 RocksDB/LSM index |
| 单机 active consumer 数 | 1000 | 多 proxy coordinator |
| 单条消息大小 | 4MB | 大消息分片或对象存储 |
| physical shard 数 | 4-16 | 根据磁盘和 CPU 核数调优 |

这些不是协议上限，而是 LocalStorageEngine MVP 的工程上限。压测和验收应围绕该边界进行。

### 9.8 文件布局

单机 MVP 使用本地文件存储，目录由 `proxy.storePath` 指定：

```text
store/
  metadata/
    topics.json
    topics.json.tmp
  physical/
    shard-0/
      segment-00000000000000000000.log
      segment-00000000001073741824.log
    shard-1/
      segment-00000000000000000000.log
  index/
    TopicA/
      partition-0.idx
      partition-1.idx
  offsets/
    GroupA/
      TopicA.offsets
      TopicA.offsets.tmp
  retry/
    retry.log
    retry.index
  dlq/
    GroupA/
      dlq.log
  checkpoint
```

文件说明：

| 文件 | 说明 |
| --- | --- |
| topics.json | topic 元数据快照 |
| physical/shard-N/segment-X.log | 共享物理消息日志段 |
| index/{topic}/partition-N.idx | 逻辑 offset 到物理位置的索引 |
| TopicA.offsets | 某 group 下 TopicA 的 partition committed offset |
| retry.log | 待重试记录追加日志 |
| retry.index | 可选，到期时间索引；MVP 可以先内存加载后扫描 |
| dlq.log | 死信消息追加日志 |
| checkpoint | 最近一次 flush 位置和时间 |

`*.tmp` 文件用于原子替换快照。写入方式是先写 tmp，fsync 后 rename 覆盖正式文件。

物理 shard 数由配置控制：

```properties
proxy.localStorePhysicalShardCount=4
proxy.localStoreSegmentBytes=1073741824
```

shard 选择策略：

```text
physicalShard = hash(topic + ":" + partitionId) % physicalShardCount
```

同一逻辑 partition 的消息稳定进入同一个 physical shard，写入仍是顺序追加；不同 topic/partition 共享少量物理文件，避免 topic 多时数据文件爆炸。

### 9.9 TopicMetadataStore

`topics.json` 保存所有 topic 元数据：

```json
{
  "topics": {
    "TopicA": {
      "topic": "TopicA",
      "partitionCount": 4,
      "createTimestamp": 1710000000000
    }
  }
}
```

规则：

1. `createTopic` 如果 topic 已存在，返回 `TOPIC_ALREADY_EXISTS`。
2. `partitionCount <= 0` 时使用 `proxy.defaultPartitionCount`。
3. topic 名称只允许字母、数字、下划线、中划线、百分号和点，最大长度 255。
4. 创建 topic 时只写 metadata 和 topic index 目录，不预创建所有 partition index 文件。
5. 第一版不支持删除 topic 和修改 partitionCount。

注意：创建 topic 不创建独立数据 log 文件，也不预创建全部 partition index。physical log shard 在 storage 启动时统一创建，所有 topic/partition 共享；logical index 在第一次 append 到对应 partition 时惰性创建。

### 9.10 MessageRecord 内部模型

```java
class MessageRecord {
    String messageId;
    String topic;
    int partitionId;
    long offset;
    String key;
    Map<String, String> properties;
    byte[] body;
    long bornTimestamp;
    long storeTimestamp;
    String producerId;
    String requestId;
    int bodyCrc;
}
```

`messageId` 由 proxy 生成，建议格式：

```text
{topic}-{partitionId}-{offset}
```

如果后续需要全局唯一且隐藏 topic 信息，可以再换成 snowflake 或 UUID，但第一版用可读 ID 更利于调试。

### 9.11 PhysicalLog 记录格式

第一版记录格式：

```text
magic(4)
version(2)
recordLength(4)
headerLength(4)
offset(8)
bornTimestamp(8)
storeTimestamp(8)
bodyCrc(4)
partitionId(4)
messageIdLength(2)
topicLength(2)
keyLength(2)
propertiesLength(4)
producerIdLength(2)
requestIdLength(2)
bodyLength(4)
messageId
topic
key
propertiesJson
producerId
requestId
body
```

字段约定：

1. `recordLength` 是从 magic 开始到 body 结束的完整记录长度。
2. `headerLength` 用于后续扩展，旧版本读取器可以跳过未知头部。
3. `bodyCrc` 只校验 body；恢复时也可以用 recordLength 判断完整性。
4. 记录内必须保存 topic 和 partitionId，因为 physical log 是多 topic/partition 共享的。
5. properties 使用 JSON 编码，MVP 不做二进制优化。
6. `offset` 字段表示 logical offset，不是 physical offset。

### 9.11.1 PhysicalLogSegment 滚动

每个 physical shard 由多个 segment 组成。segment 文件名中的数字是该 segment 在 shard 内的 base physical offset：

```text
segment-00000000000000000000.log
segment-00000000001073741824.log
```

滚动规则：

1. active segment 剩余空间小于待写 recordLength 时，创建新 segment。
2. 新 segment base offset = 当前 segment base offset + 当前 segment wrotePosition。
3. segment base offset 必须单调递增。
4. 单条消息大小不能超过 `proxy.localStoreSegmentBytes`，否则返回 `INVALID_REQUEST`。
5. segment 滚动时先创建新文件并 fsync 目录，再切换 active segment。

`PhysicalLogShard.append` 伪代码：

```java
append(recordBuffer):
    appendLock.lock()
    try:
        if activeSegment.remaining() < recordBuffer.remaining():
            rollSegment()
        position = activeSegment.wrotePosition()
        activeSegment.write(recordBuffer)
        return new PhysicalAppendResult(shardId, activeSegment.baseOffset(), position, recordLength)
    finally:
        appendLock.unlock()
```

第一版不做 segment 删除。后续加保留策略时，只有所有 consumer group committed offset 都越过某段数据，且 retry/DLQ 不再引用该段，才能删除。

### 9.12 LogicalQueueIndex 格式

每个 logical queue index 文件对应一个 `topic + partition`，记录逻辑 offset 到物理 log 的映射。

索引格式：

```text
logicalOffset(8)
physicalShardId(4)
physicalSegmentBaseOffset(8)
physicalPosition(8)
recordLength(4)
storeTimestamp(8)
```

索引规则：

1. 每条消息对应一条固定长度 index 记录，MVP 为 40 bytes。
2. `logicalOffset` 从 0 开始单调递增。
3. `physicalPosition` 是 physical segment 文件中的字节偏移。
4. `maxOffset` 采用 exclusive 语义，即下一条待写 offset。
5. 如果 index 缺失或损坏，可以从 physical log 顺序扫描目标 topic/partition 重建，但成本较高，所以正常关闭必须 flush index。

这种设计仍然会有每个 topic partition 一个 index 文件，但 index 文件固定长度、小且只保存定位信息，压力远小于每个 partition 一套 data log + index。后续如果 topic 数量极大，可以把 logical index 再合并为 index shard 或迁移到 RocksDB/LSM。

### 9.12.1 LogicalQueueIndex 读写细节

index 文件固定长度，按 offset 直接定位：

```text
indexPosition = logicalOffset * INDEX_ENTRY_SIZE
```

读取一批消息时，先连续读取 index entries，再根据 entry 读取 physical log：

```java
entries = index.read(offset, maxMessages);
for (IndexEntry entry : entries) {
    record = physicalLog.read(entry.physicalPosition, entry.recordLength);
}
```

写 index 时必须校验：

1. `entry.logicalOffset == currentMaxOffset`。
2. index 文件当前位置等于 `logicalOffset * INDEX_ENTRY_SIZE`。
3. 写成功后才更新内存 `maxOffset`。

如果写 index 失败，physical log 中可能存在孤儿记录。此时该 logical queue 必须立即进入 `FENCED` 状态，拒绝后续 append，直到 LocalStorageEngine 重启恢复或显式 repair 完成。不能继续给同一个 logical offset 写入新消息，否则 index 丢失后重建可能把旧 orphan record 绑定到该 offset，导致 offset 对应的消息发生变化。

第一版不实现 commit marker。可见性规则是：只有 physical log 和 logical index 都写成功的消息才对 fetch 可见；physical log 中没有 index 的记录只作为恢复输入，不作为在线读取结果。

### 9.13 Append 流程

`append(AppendRequest)` 流程：

```text
1. 校验 topic 存在。
2. 校验 partitionId 在 [0, partitionCount)。
3. logicalQueue = topic + partitionId。
4. 获取该 LogicalQueueIndex 的 offset 锁。
5. logicalOffset = currentMaxOffset。
6. 选择 physicalShard = hash(topic + ":" + partitionId) % shardCount。
7. 获取 PhysicalLogShard 写锁。
8. 补齐 record.topic、partitionId、offset、storeTimestamp、messageId、bodyCrc。
9. 编码 record 为 byte buffer。
10. 追加写 physical segment，得到 physicalSegmentBaseOffset 和 physicalPosition。
11. 追加写 logical queue index。
12. 更新 logical queue 内存 maxOffset = logicalOffset + 1。
13. 根据 flush 策略决定是否 force。
14. 返回 AppendResult。
```

写入顺序必须是 **先 physical log 后 logical index**。如果进程在 physical log 写完但 index 未写完时崩溃，恢复时可以扫描 physical log 重建 index。不能先写 index，否则可能出现 index 指向不存在或半条 log 的记录。

第一版每个 logical queue 一把 offset 锁，保证同一 topic/partition 内 offset 连续。每个 physical shard 一把写锁，保证同一个 shard 顺序追加。不同 physical shard 可以并发写。

### 9.13.1 Append 失败矩阵

| 失败点 | 结果 | 恢复策略 |
| --- | --- | --- |
| physical log 写前失败 | 没有可见消息 | 直接返回错误 |
| physical log 写半条失败 | 可能有半条记录 | 启动恢复截断到上一条完整记录 |
| physical log 写成功，index 写前失败 | 有孤儿 physical record，该 logical queue 状态未知 | 立即 fence 该 logical queue，拒绝后续 append；启动恢复扫描 physical log 重建 index |
| index 写半条失败 | index 尾部不完整，该 logical queue 状态未知 | 立即 fence 该 logical queue；启动恢复截断 index 后重建缺失项 |
| index 写成功，返回前失败 | 消息已可见但客户端可能重试 | send 幂等缓存命中时返回同一结果；缓存丢失时可能重复，符合 MVP 约束 |

append 返回成功的条件是 physical log 和 logical index 都写入成功。SYNC 模式下还要求两者 force 成功。

如果 logical queue 被 fence，`append` 返回 `STORAGE_ERROR`，错误信息必须包含 topic、partitionId 和 fenced reason。MVP 不做在线 repair，运维动作是重启 LocalStorageEngine 触发恢复。

Producer 可见性约束：

1. append 返回成功：消息必须可恢复、可读取。
2. append 返回失败：消息可能不可见，也可能在恢复后变为可见，因为失败可能发生在 physical log 持久化之后。
3. 因此 SDK 重试必须复用同一个 `producerId + requestId`。
4. MVP 的 send dedup 在 proxy 重启后可能丢失，导致失败重试出现重复消息；这属于 at-least-once producer 语义。
5. 生产版本如果要收紧语义，必须把 send dedup 结果持久化。

### 9.14 Fetch 流程

`fetch(FetchRequest)` 流程：

```text
1. 校验 topic 和 partitionId。
2. 如果 request.offset < minOffset，返回空结果并标记 nextOffset = minOffset。
3. 如果 request.offset >= maxOffset，返回空结果，nextOffset = maxOffset。
4. 从 logical queue index 定位 request.offset 对应 physical shard、segment 和 position。
5. 顺序读取 physical log 记录。
6. 校验 magic、recordLength、bodyCrc。
7. 累计 records，直到达到 maxMessages 或 maxBytes。
8. 每读一条后继续读取下一条 logical index，而不是盲扫 physical log。
9. 返回 records、nextOffset、minOffset、maxOffset。
```

第一版没有消息删除，所以：

```text
minOffset = 0
maxOffset = next append offset
```

`FetchResult.nextOffset` 是下一次 fetch 应传入的 offset。如果没有读到消息，`nextOffset = maxOffset`。

### 9.15 ConsumerOffsetStore

offset 文件按 group/topic 存储：

```text
offsets/
  GroupA/
    TopicA.offsets
```

内容为 JSON：

```json
{
  "group": "GroupA",
  "topic": "TopicA",
  "offsets": {
    "0": 123,
    "1": 456
  },
  "updateTimestamp": 1710000000000
}
```

约定：

1. committed offset 表示下一条应消费 offset，而不是最后一条已消费 offset。
2. 不存在 offset 时返回 0。
3. `commitConsumerOffset` 只允许提交大于等于当前 committed offset 的值。
4. 快照写入使用 tmp + fsync + rename。
5. MVP 可以每次 commit 都写快照；后续再做批量 flush。

### 9.16 RetryStore

retry 记录不复制消息 body，只保存原消息引用：

```java
class RetryRecord {
    String group;
    String topic;
    int partitionId;
    long offset;
    String messageId;
    int deliveryAttempt;
    long nextVisibleTime;
    String reason;
    RetryStatus status;
}
```

`RetryStatus`：

```text
PENDING
DELIVERING
DONE
DLQ
```

MVP 实现可以简化：

1. `retry.log` 追加写 RetryRecord。
2. 启动时全量加载 retry.log 到内存 priority queue。
3. RetryDispatcher 扫描到期记录。
4. 到期后通过原 topic/partition/offset 查询 LogicalQueueIndex，再从 PhysicalLog 读取原消息。
5. 投递成功进入 inflight，不立即标记 DONE；ack 后由 RetryService 标记 DONE。
6. retry.log 的 DONE/DLQ 状态也用追加事件记录，定期 compact 后续再做。

第一版可以不实现 retry compact，但测试要覆盖重启后不会丢失 PENDING retry。

### 9.17 DlqStore

DLQ 记录保存完整消息快照和失败上下文，避免原始 topic 后续清理后无法排查：

```java
class DlqRecord {
    String group;
    String originalTopic;
    int originalPartitionId;
    long originalOffset;
    String messageId;
    int deliveryAttempt;
    String reason;
    long storeTimestamp;
    MessageRecord messageSnapshot;
}
```

MVP 中 DLQ 写到：

```text
dlq/{group}/dlq.log
```

后续可以把 DLQ 暴露成系统 topic：`%DLQ%{group}`。

### 9.18 Flush 策略

第一版支持两个配置：

```properties
proxy.localStoreFlushMode=SYNC
proxy.localStoreFlushIntervalMillis=1000
```

模式：

| 模式 | 行为 | 语义 |
| --- | --- | --- |
| SYNC | append 后 force physical log 和 logical index，再返回 SendResponse 成功 | send success 后可恢复，吞吐低 |
| ASYNC | append 后写入 OS page cache，后台定期 force | 吞吐高，异常断电可能丢失已返回成功的消息 |

MVP 默认 `SYNC`。这是第一版的 durability contract：**只要 SendResponse 返回成功，消息在进程崩溃后必须能通过恢复重新读取**。

`ASYNC` 只能作为显式性能模式开启。开启后语义降级为：进程崩溃通常可恢复，机器断电或 OS page cache 丢失时，可能丢失已经返回成功的消息。该模式不能用于可靠性测试和生产默认配置。

无论哪种模式，正常 shutdown 必须 flush：

1. 所有 physical log segment。
2. 所有已打开 logical queue index。
3. offset 快照。
4. retry/dlq log。
5. checkpoint。

### 9.18.1 StoreCheckpoint

checkpoint 文件保存最近一次 flush 到磁盘的物理位置、logical index 进度和时间。MVP 中 checkpoint **只用于诊断和后续优化，不用于跳过恢复扫描**。启动恢复仍以 physical log 和 logical index 的实际校验结果为准，避免 checkpoint 与 index 不一致时漏恢复。

格式：

```json
{
  "flushTimestamp": 1710000000000,
  "physicalShards": {
    "0": {
      "activeSegmentBaseOffset": 1073741824,
      "flushedPosition": 123456
    },
    "1": {
      "activeSegmentBaseOffset": 0,
      "flushedPosition": 987654
    }
  },
  "logicalQueues": {
    "TopicA#0": {
      "maxOffset": 123,
      "indexFileSize": 4920
    },
    "TopicA#1": {
      "maxOffset": 456,
      "indexFileSize": 18240
    }
  }
}
```

checkpoint 写入规则：

1. 先 flush physical log。
2. 再 flush logical index。
3. 再 flush offset/retry/dlq。
4. 最后写 checkpoint tmp，fsync 后 rename。

如果 checkpoint 丢失、损坏，或与实际文件状态不一致，启动恢复退化为全量扫描 physical log 和校验 logical index。MVP 不允许仅凭 checkpoint 跳过 physical log 尾部校验。

### 9.19 并发模型

1. 每个 LogicalQueueIndex 一把 offset 锁，用于分配连续 logical offset。
2. 每个 PhysicalLogShard 一把写锁，用于保证 shard 内顺序追加。
3. fetch 使用读路径，不阻塞其他 logical queue 写入。
4. 同一 logical queue fetch 和 append 可以并发，但读取 offset 必须小于当前可见 maxOffset。
5. logical index 追加后才提升内存 maxOffset，避免 fetch 读到尚无 index 的记录。
6. TopicMetadataStore createTopic 使用全局元数据锁。
7. ConsumerOffsetStore 每个 group/topic 一把写锁。

第一版不支持多个 JVM 同时打开同一个 `proxy.storePath`。启动时创建：

```text
store/lock
```

并通过文件锁避免多进程同时写入。

### 9.20 启动恢复

LocalStorageEngine 启动时执行：

```text
1. 获取 store/lock 文件锁。
2. 加载 topics.json；不存在则创建空 metadata。
3. 打开固定数量的 physical shard 和 segment。
4. 扫描每个 physical segment，找到最后一条完整且 crc 正确的记录。
5. 如果尾部有半条或 crc 错误，截断 segment 到最后有效位置。
6. 打开每个 topic/partition 的 logical index。
7. 校验 logical index 是否连续且 physical position 指向有效 physical record。
8. 如果 index 缺失、长度不对或内容不匹配，从 physical log 扫描对应 topic/partition 重建 index。
9. 设置每个 logical queue 的 minOffset=0，maxOffset=最后有效 logical offset + 1。
10. 加载 offsets 快照。
11. 加载 retry.log，重建内存 retry priority queue。
12. 打开 dlq.log。
```

恢复后的关键保证：

1. 不读取半条消息。
2. logical index 与 physical log 一致。
3. maxOffset 不越过最后完整消息。
4. 已提交 offset 如果大于 partition maxOffset，降到 maxOffset 并记录 warn 日志。

### 9.20.1 Logical Index 重建算法

当 logical index 缺失或损坏时，从 physical log 重建：

```text
1. 初始化 rebuildState: Map<LogicalQueueKey, nextExpectedOffset>
2. 按 shardId 遍历 physical shard
3. 按 segmentBaseOffset 升序遍历 segment
4. 顺序读取每条 physical record
5. 校验 magic、recordLength、bodyCrc
6. 解析 topic、partitionId、logicalOffset
7. 如果 topic 不存在，跳过并记录 warn
8. 如果 partitionId 超出 topic partitionCount，跳过并记录 warn
9. 读取该 logical queue 当前 index maxOffset 作为 nextExpectedOffset
10. 如果 logicalOffset < nextExpectedOffset，说明 index 已有该记录，跳过
11. 如果 logicalOffset == nextExpectedOffset，追加 index entry，nextExpectedOffset++
12. 如果 logicalOffset > nextExpectedOffset，说明 logical queue 中间缺失，记录 error；MVP 停止该 queue 的重建并标记 store corrupted
```

为什么遇到 gap 要 fail fast：logical offset 缺口会导致 consumer 永远无法连续推进 committed offset。MVP 不自动跳洞，避免静默丢消息。

### 9.20.2 恢复策略选择

启动时根据文件状态选择恢复路径：

| 状态 | 恢复策略 |
| --- | --- |
| checkpoint 正常，所有 index 正常 | 仍扫描 physical log 尾部并校验 index；checkpoint 仅用于日志和诊断 |
| checkpoint 缺失 | 全量扫描 physical log 校验尾部，校验 index |
| 某 index 缺失 | 只重建该 logical queue index |
| 某 index 尾部半条 | 截断 index 后，从 physical log 补齐 |
| physical segment 尾部半条 | 截断 physical segment，重建受影响 index |
| physical record crc 错误 | 截断到上一条完整记录；如果不是尾部错误，标记 store corrupted |

MVP 只需要保证尾部损坏可恢复；中间损坏直接失败并要求人工处理。

### 9.21 异常和错误映射

| 场景 | Storage 异常 | gRPC ErrorCode |
| --- | --- | --- |
| topic 不存在 | TopicNotFoundException | TOPIC_NOT_FOUND |
| partition 不存在 | InvalidRequestException | INVALID_REQUEST |
| body 超过限制 | InvalidRequestException | INVALID_REQUEST |
| crc 校验失败 | StoreCorruptedException | STORAGE_ERROR |
| 文件锁获取失败 | StoreAlreadyOpenedException | INTERNAL_ERROR |
| 磁盘写失败 | StoreIOException | STORAGE_ERROR |
| fetch offset 小于 minOffset | OffsetOutOfRangeException | INVALID_REQUEST 或返回 minOffset |

MVP 中 fetch offset 小于 minOffset 暂不报错，因为没有清理，minOffset 固定为 0。

### 9.22 LocalStorageEngine 测试清单

必须覆盖：

1. createTopic 创建 metadata，不预创建所有 logical index 文件。
2. append 单条后 fetch 返回同一条消息。
3. append 多条后按 offset fetch，nextOffset 正确。
4. maxMessages 和 maxBytes 生效。
5. 同 key 消息由上层 selector 选择到同 partition 后 offset 连续。
6. logical index 删除后重启可从 physical log 重建。
7. physical segment 尾部写入半条后重启会截断。
8. crc 错误后重启截断到上一条有效记录。
9. commit offset 后重启仍能查询到。
10. retry PENDING 记录重启后仍会被扫描到。
11. DLQ 写入后文件中存在完整记录。
12. 第二个 LocalStorageEngine 打开同一 storePath 失败。
13. 多 topic 写入时 physical shard 文件数不随 topic 数线性增长。
14. segment 超过 `proxy.localStoreSegmentBytes` 后自动滚动。
15. logical index LRU 淘汰文件前会 flush。
16. index 尾部半条记录重启后可截断并补齐。
17. physical log 中出现 logical offset gap 时启动失败并报告 store corrupted。
18. checkpoint 损坏时可退化为全量扫描恢复。
19. index 写失败后 logical queue 进入 FENCED，后续 append 被拒绝。
20. append 返回失败但 physical log 已写入时，重启恢复后消息可能可见，测试必须验证不会复用同一 logical offset 写入另一条消息。

这些测试通过后，LocalStorageEngine 才能支撑阶段一集成测试。

## 10. Consumer Group 与分配

### 10.1 Membership

consumer 加入 group 后，proxy 写入 membership lease：

```text
group
clientId
sessionId
topics
lastHeartbeatTime
leaseExpireTime
```

session 正常关闭或 lease 过期都会触发重新分配。

### 10.2 第一版分配策略

第一版使用简单平均分配：

```text
sort(partitions)
sort(activeConsumers)
partitionIndex % consumerCount
```

优点是确定性强，便于测试和排查。后续可以替换为 sticky assignor，减少 consumer 上下线时的 partition 抖动。

### 10.3 Rebalance 处理

发生 rebalance 时：

1. assignment version 增加。
2. 老 owner 停止被撤销 partition 的新投递。
3. 老 owner 的 inflight 等待 ack 或超时。
4. 新 owner 从 committed offset 开始读取。
5. 允许少量重复投递，保证不丢消息。

第一版不追求无重复 rebalance，重点是正确性和可恢复。

### 10.4 单机 MVP 的简化

单机 MVP 可以先不实现真正的跨节点 group membership。仍然按完整模型写接口，但实现上：

- activeConsumers 存在当前进程内存。
- assignment version 由本机 AtomicLong 生成。
- consumer 断连立即触发本机 rebalance。
- proxy 重启后所有 consumer 重新 subscribe，从持久化 committed offset 恢复。

这样不会阻塞 SDK、协议、storage 和 offset 主链路开发。

## 11. 顺序消费预留

第一版不实现顺序消费，但保留以下设计点：

1. SDK send API 支持 key。
2. 消息存储包含 key、partitionId、offset。
3. offset 按 `group + topic + partition` 管理。
4. proxy 投递内部始终带 partition 维度。
5. PartitionSelector 接口化。
6. DeliveryService 支持按 partition 切换投递策略。

后续支持顺序消费时新增配置：

```text
consumeMode = CONCURRENT | ORDERLY
```

ORDERLY 模式规则：

- same key 固定到 same partition。
- same partition 只分配给一个 consumer session。
- same partition 串行投递，或按有序 batch 投递。
- SDK 对 same partition 使用单线程执行器。
- nack 或超时时暂停该 partition 后续投递，直到 retry 处理完成。

只要第一版保留上述数据模型，后续改动主要集中在 PartitionSelector、DeliveryService 和 SDK executor，不需要推翻协议和存储模型。

## 12. Retry 与 DLQ

### 12.1 Retry 记录

retry 记录包含：

```text
group
topic
partitionId
offset
messageId
deliveryAttempt
nextVisibleTime
reason
```

RetryService 周期扫描到期记录，重新生成投递任务。

### 12.2 DLQ

超过最大重试次数后写入 DLQ：

```text
%DLQ%{group}
```

DLQ 消息保留原始 topic、partitionId、offset、messageId 和失败原因，便于后续排查和重新投递。

### 12.3 第一版简化

第一版可以先使用固定重试间隔：

```text
retryDelay = 10s
maxDeliveryAttempts = 16
```

后续再支持多级退避、按 topic/group 配置重试策略。

## 13. 配置设计

### 13.1 Proxy 配置

```properties
proxy.backendType=custom
proxy.grpcListenPort=10920
proxy.grpcMaxMessageBytes=4194304
proxy.metadataStore=local
proxy.storageEngine=local
proxy.storePath=/tmp/mq-proxy-store
proxy.localStorePhysicalShardCount=4
proxy.localStoreSegmentBytes=1073741824
proxy.localStoreMaxOpenIndexFiles=1024
proxy.localStoreFlushMode=SYNC
proxy.localStoreFlushIntervalMillis=1000
proxy.defaultPartitionCount=4
proxy.consumerLeaseMillis=30000
proxy.ackTimeoutMillis=30000
proxy.maxDeliveryAttempts=16
proxy.retryDelayMillis=10000
proxy.ackTokenSecret=local-development-secret
```

`backendType=rocketmq` 可以保留当前 RocketMQ adapter 链路；`backendType=custom` 启用新计算层和自研存储。

### 13.2 SDK 配置

```java
MqClientConfig config = new MqClientConfig();
config.setProxyAddresses("127.0.0.1:10920");
config.setRequestTimeoutMillis(3000);
config.setMaxInflight(256);
config.setConcurrency(8);
config.setAckTimeoutMillis(30000);
config.setEnableMetrics(true);
```

### 13.3 默认值

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| proxy.grpcListenPort | 10920 | 新 gRPC 入口 |
| proxy.grpcMaxMessageBytes | 4MB | 单条 gRPC 消息上限 |
| proxy.localStorePhysicalShardCount | 4 | 本地存储共享物理日志 shard 数 |
| proxy.localStoreSegmentBytes | 1GB | 单个 physical segment 文件大小 |
| proxy.localStoreMaxOpenIndexFiles | 1024 | logical index 文件句柄 LRU 上限 |
| proxy.localStoreFlushMode | SYNC | 本地存储刷盘模式，默认保证 send success 后可恢复 |
| proxy.localStoreFlushIntervalMillis | 1000 | ASYNC 模式刷盘间隔 |
| proxy.defaultPartitionCount | 4 | create topic 未指定时使用 |
| proxy.consumerLeaseMillis | 30000 | consumer 心跳租约 |
| proxy.ackTimeoutMillis | 30000 | 投递后未 ack 的超时时间 |
| proxy.maxDeliveryAttempts | 16 | 最大投递次数 |
| proxy.retryDelayMillis | 10000 | MVP 固定重试延迟 |
| sdk.maxInflight | 256 | 单 consumer 本地最大未 ack |
| sdk.concurrency | 8 | callback 并发度 |

## 14. 与现有 mq-proxy 的演进关系

当前项目已有 `StorageAdapter`、`MessageEngine`、`VirtualRouteManager` 等抽象，但这些抽象仍带有 RocketMQ broker 模型，例如 brokerAddr、NameServer route、forwardToBroker。

新架构建议新增一条 gRPC 轻 SDK 链路：

```
mq-proxy-sdk-v2
mq-proxy-grpc
mq-proxy-compute
mq-proxy-storage-api
mq-proxy-storage-local
```

现有 RocketMQ 兼容链路继续保留：

```
mq-proxy-core
mq-proxy-rocketmq
mq-proxy-standalone
```

两条链路通过启动配置选择，避免在现有 processor 中散落大量 `if rocketmq/custom` 分支。

### 14.1 Maven 依赖方向

建议依赖方向：

```text
mq-proxy-sdk-v2
  -> mq-proxy-grpc generated stubs

mq-proxy-grpc
  -> mq-proxy-compute
  -> mq-proxy-storage-api

mq-proxy-storage-local
  -> mq-proxy-storage-api

mq-proxy-standalone
  -> mq-proxy-grpc
  -> mq-proxy-storage-local
```

`mq-proxy-compute` 不依赖 gRPC 生成类，避免计算层被协议绑定。gRPC mapper 负责在 proto model 和 compute model 之间转换。

### 14.2 和旧 StorageAdapter 的关系

旧 `StorageAdapter` 暂时不改，继续服务 RocketMQ 兼容链路。新链路使用新的 `StorageEngine`，原因是：

- `StorageAdapter` 参数里有 brokerAddr，绑定 RocketMQ broker 模型。
- `forwardToBroker` 是 RocketMQ 后端专用能力。
- 新存储层需要 partition log 语义，而不是代理到 broker 地址。

后续如果需要统一，可以在更高层抽象 `BackendRuntime`，但第一版不要为了统一接口牺牲清晰边界。

## 15. 分阶段交付

### 15.1 阶段一：单机 MVP

目标：跑通新 SDK 到 proxy 到本地存储的完整链路。

范围：

1. Java SDK。
2. gRPC Send、BatchSend、Consume。
3. callback 消费。
4. maxInflight + credit 流控。
5. 本地 partition log 存储。
6. committed offset 持久化。
7. ack/nack。
8. 固定延迟 retry。
9. 简单 DLQ。

验收条件：

1. 创建 topic 后，SDK send 返回 messageId、partitionId、offset。
2. SDK callback consumer 能收到 send 的消息。
3. listener ack 后，重启 proxy 不再消费已持久化 committed offset 之前的消息。
4. listener 不 ack，超过 ackTimeout 后消息被再次投递，deliveryAttempt 增加。
5. listener nack 后消息进入 retry，到期后再次投递。
6. 超过 maxDeliveryAttempts 后写入 DLQ。
7. maxInflight=1 时，同一 consumer 本地最多只有 1 条未 ack 消息。
8. 有 key 的消息稳定落到同一 partition。
9. partition log 尾部损坏时，重启能截断不完整记录并继续启动。

### 15.2 阶段二：多 proxy 计算层

目标：支持 proxy 横向扩容。

范围：

1. membership lease 持久化。
2. group partition assignment version。
3. proxy 节点上下线处理。
4. rebalance 正确性测试。
5. SDK 多 proxy 地址和故障切换。

### 15.3 阶段三：增强语义

目标：补齐生产能力。

范围：

1. 顺序消费。
2. 更完整 retry 策略。
3. 存储副本和故障恢复。
4. 指标、trace、管理 API。
5. backpressure 按字节和延迟动态调整。

## 16. 测试策略

### 16.1 单元测试

1. PartitionSelector：key hash 稳定性、无 key round-robin。
2. OffsetService：乱序 ack 下 committed offset 推进。
3. InflightTracker：ackToken 校验、重复 ack、过期 token。
4. RetryService：nack、超时、超过最大次数进入 DLQ。
5. ConsumerCoordinator：成员变化后的分配确定性。

### 16.2 集成测试

1. SDK send 后 consumer 能收到消息。
2. consumer ack 后重启 proxy 不重复消费已提交消息。
3. consumer 不 ack 后超时重投。
4. nack 后进入 retry。
5. maxInflight 生效，本地不超过配置上限。
6. 多 consumer 同 group 下 partition 分配正确。
7. poison message 超过最大重试次数进入 DLQ 后，committed offset 可以继续推进。
8. `ConsumeResult.DROP` 会写 DLQ/drop 记录，并把原 offset 标记完成。

### 16.3 压测与故障测试

1. 单 topic 多 partition 发送吞吐。
2. callback 并发消费吞吐。
3. proxy 重启恢复。
4. SDK stream 断开重连。
5. storage append/fetch 延迟上升时的 backpressure 行为。

### 16.4 必须先写的测试用例

开发顺序建议以这些测试倒推：

1. `LocalStorageEngineTest`：append 后按 offset fetch。
2. `LocalStorageEngineRecoveryTest`：尾部半条记录恢复。
3. `OffsetServiceTest`：ack offset 10 但 9 未 ack 时不能推进。
4. `InflightTrackerTest`：ackToken 过期、重复 ack、错误签名。
5. `PartitionSelectorTest`：同 key 分区稳定。
6. `ProducerComputeServiceTest`：同 producerId/requestId 重试返回同一结果。
7. `GrpcConsumerIntegrationTest`：SDK subscribe 后完成 send -> deliver -> ack。
8. `RetryIntegrationTest`：nack -> retry -> DLQ。
9. `PoisonMessageOffsetAdvanceTest`：毒丸消息 DLQ 后不阻塞后续 offset 提交。
10. `DropConsumeResultTest`：DROP 记录可追踪且 offset 可推进。

这些测试通过前，不进入多 proxy 阶段。

## 17. 关键设计结论

1. 第一版使用 gRPC，而不是 RocketMQ RemotingCommand 或自定义 TCP。
2. 用户主 API 使用 callback，辅助提供 poll。
3. 底层消费协议使用 bidirectional stream。
4. push 投递必须由 credit 控制，不能无上限推送。
5. proxy 管理 consumer group、partition、offset、retry、DLQ。
6. SDK 不暴露 queueId、brokerName、offset。
7. 存储层只提供 partition log 能力，不承担消费调度。
8. 第一版语义是 at-least-once。
9. key、partitionId、offset 从第一版进入数据模型，为顺序消费预留。
10. 新架构作为独立 gRPC 链路演进，避免污染现有 RocketMQ 兼容链路。

## 18. 开发落地检查清单

开始编码前需要确认以下事项已经按本文定稿：

1. proto 字段和 ErrorCode 不再随意调整。
2. 第一版只做单机 MVP，不做多 proxy 强一致。
3. 新链路使用 `StorageEngine`，不复用旧 `StorageAdapter`。
4. SDK 主 API 是 callback，poll 是辅助 API。
5. Send 重试必须复用 requestId。
6. Offset 按 `group + topic + partition` 管理。
7. key、partitionId、offset 必须进入第一版消息模型。
8. LocalStorageEngine 必须支持 crash recovery。
9. ackToken 使用签名，SDK 不解析 token。
10. maxInflight 和 credit 是消费链路必选能力，不是优化项。
