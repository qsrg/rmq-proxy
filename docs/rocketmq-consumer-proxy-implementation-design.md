# RocketMQ 消费者请求代理落地实现设计

## 1. 背景与目标

本项目目标是让 RocketMQ 原生客户端无感接入 proxy，链路为：

```text
RocketMQ client -> proxy -> RocketMQ broker
RocketMQ broker -> proxy -> RocketMQ client
```

生产请求大多可以按请求/响应模型转发，消费者链路更复杂。RocketMQ 的消费组成员管理、rebalance 触发、顺序消费队列锁、offset 查询更新都依赖 broker 与 client 之间的双向 remoting 语义。proxy 如果只做单向请求转发，会在多 proxy、多 consumer、consumer 下线、broker 路由变化时破坏原生 rebalance 语义。

本文档基于 RocketMQ 4.9.8 源码语义和当前项目实现，定义消费者请求代理的目标架构、关键时序、代码改造点和测试矩阵。

## 2. RocketMQ 原生消费者链路

RocketMQ 4.9.8 中，consumer rebalance 是客户端执行的，broker 只维护消费组成员并触发通知。

核心源码链路如下：

```text
consumer HEART_BEAT
  -> broker ClientManageProcessor.heartBeat
  -> ConsumerManager.registerConsumer
  -> ConsumerIdsChangeListener.handle(CHANGE)
  -> Broker2Client.notifyConsumerIdsChanged
  -> client ClientRemotingProcessor.notifyConsumerIdsChanged
  -> MQClientInstance.rebalanceImmediately
  -> RebalanceImpl.doRebalance
  -> MQClientInstance.findConsumerIdList
  -> MQClientAPIImpl.getConsumerIdListByGroup
  -> broker 返回 group 全局 consumerIdList
  -> client AllocateMessageQueueStrategy.allocate
```

对应语义：

| 环节 | 权威方 | 说明 |
|------|--------|------|
| consumer 本地运行状态 | consumer | pull 状态、ProcessQueue、rebalance 执行都在客户端 |
| group 成员表 | broker | `ConsumerManager` 维护 group 到 client/channel 的映射 |
| rebalance 触发 | broker | broker 通过 `NOTIFY_CONSUMER_IDS_CHANGED` 通知客户端 |
| rebalance 计算 | consumer | consumer 获取全局成员列表后本地计算队列分配 |
| consumer list | broker | `GET_CONSUMER_LIST_BY_GROUP` 必须返回全局成员视图 |

因此 proxy 不能把自己变成消费组成员权威。正确方向是：broker 仍然负责全局成员视图，proxy 负责连接映射和双向请求转发。

## 3. 目标架构

proxy 在消费者链路上同时扮演两个角色：

```text
真实 consumer 视角:
  proxy 是 broker

真实 broker 视角:
  proxy 是一组真实 consumer client 的连接代理
```

proxy 需要维护两类会话。

### 3.1 下游会话

下游会话表示真实 consumer 到 proxy 的连接。

```text
clientId -> downstream channel
channel -> ClientInfo
consumerGroup -> local clientIds
```

职责：

- 记录真实 clientId、consumerGroup、subscription、messageModel、consumeType。
- 接收真实 consumer 的 `HEART_BEAT` 和 `UNREGISTER_CLIENT`。
- 在 broker 反向请求到来时，根据 clientId 找到真实 consumer channel。
- 在 channel inactive 时触发本地状态清理和上游注销。

当前 `ClientConnectionManager` 已经承担了大部分职责，应明确为下游会话表。

### 3.2 上游会话

上游会话表示 proxy 到 broker 的连接，但它的语义是“代表某个真实 clientId”。

```text
clientId -> upstream NettyRemotingClient
clientId + brokerAddr -> upstream channel
```

职责：

- 使用真实 clientId 构造 `HeartbeatData` 发给 broker。
- 使用真实 clientId 和 group 向 broker 发送 `UNREGISTER_CLIENT`。
- 注册 broker 到 client 的反向 request processor。
- 把 broker 的反向 request 转发给对应 downstream channel。
- 管理上游 channel 的生命周期和异常清理。

当前 `ProxyBrokerHeartbeatService` 已经开始按 clientId 创建 dedicated `NettyRemotingClient`，并注册 `NOTIFY_CONSUMER_IDS_CHANGED` 等反向处理器。这个方向正确，但职责已经超过 heartbeat，建议演进为独立的 `UpstreamConsumerSessionManager` 或 `BrokerSideClientSessionManager`。

### 3.3 状态权威划分

| 状态 | 权威方 | proxy 策略 |
|------|--------|------------|
| group 全局成员列表 | broker | proxy 转发 `GET_CONSUMER_LIST_BY_GROUP` 到 broker |
| 本 proxy 连接的 consumer | proxy | `ClientConnectionManager` 本地维护 |
| consumer rebalance 结果 | consumer | proxy 不参与计算 |
| broker route 虚拟化 | proxy | `VirtualRouteManager` 把真实 broker 地址改写为 proxy 地址 |
| 顺序消费队列锁 | broker | proxy 转发 `LOCK_BATCH_MQ` / `UNLOCK_BATCH_MQ` |
| offset | broker | proxy 转发 query/update offset |

## 4. 关键请求代理策略

### 4.1 HEART_BEAT

下游 consumer 发来的 `HEART_BEAT` 需要做两件事：

1. 更新本地下游会话。
2. 同步真实 clientId 的心跳到所有相关 broker。

推荐流程：

```text
consumer -> proxy: HEART_BEAT
proxy: decode HeartbeatData
proxy: ClientConnectionManager.registerConsumer/registerProducer
proxy: UpstreamConsumerSessionManager.syncHeartbeat(clientId)
proxy -> broker: HEART_BEAT(原始 clientId, group, subscription)
broker: ConsumerManager.registerConsumer
broker -> proxy: NOTIFY_CONSUMER_IDS_CHANGED(按需)
proxy -> consumer: NOTIFY_CONSUMER_IDS_CHANGED
```

实现要求：

- 心跳同步必须支持幂等更新。
- subscription 变化必须同步到 broker，因为 broker 会据此更新 filter 和 group metadata。
- `BROADCASTING` 模式不参与 clustering rebalance，但心跳仍需同步给 broker。
- 首次注册不应只等定时任务，应该事件驱动立即同步。

### 4.2 UNREGISTER_CLIENT

下游 consumer 主动关闭时会发 `UNREGISTER_CLIENT`。proxy 必须把该事件同步给 broker。

推荐流程：

```text
consumer -> proxy: UNREGISTER_CLIENT(clientId, consumerGroup)
proxy: remove downstream session
proxy -> broker: UNREGISTER_CLIENT(clientId, consumerGroup)
broker: ConsumerManager.unregisterConsumer
broker -> remaining upstream channels: NOTIFY_CONSUMER_IDS_CHANGED
proxy -> local remaining consumers: forward NOTIFY_CONSUMER_IDS_CHANGED
```

实现要求：

- 显式发送 `UNREGISTER_CLIENT` 优先于仅关闭上游 channel。
- 对所有该 clientId 注册过的 broker 发送注销。
- 注销后清理 `clientId -> upstream client/channel`。
- 如果注销失败，要记录日志并关闭上游 channel，让 broker 的 channel close/scan 兜底。

### 4.3 downstream channel inactive

异常断线时，真实 consumer 不一定能发 `UNREGISTER_CLIENT`。proxy 应在 server 侧 `channelInactive` 中主动清理。

推荐流程：

```text
proxy NettyRemotingServer.channelInactive
proxy: ClientConnectionManager.onChannelInactive
proxy: 找到该 channel 上的 clientId/groups
proxy -> broker: UNREGISTER_CLIENT for each group
proxy: close upstream session
broker -> remaining consumers: NOTIFY_CONSUMER_IDS_CHANGED
```

当前 `ClientConnectionManager.onChannelInactive` 会移除本地状态，但需要在移除前或回调参数中保留下线 client 的完整 groups 信息，供上游注销使用。

### 4.4 NOTIFY_CONSUMER_IDS_CHANGED

broker 发来的 rebalance 通知必须从上游会话转发给真实 consumer。

推荐流程：

```text
broker -> proxy upstream channel: NOTIFY_CONSUMER_IDS_CHANGED(group)
proxy: upstream session 知道该 channel 代表哪个 clientId
proxy: find downstream channel by clientId
proxy -> consumer: NOTIFY_CONSUMER_IDS_CHANGED(group)
consumer: ClientRemotingProcessor.notifyConsumerIdsChanged
consumer: MQClientInstance.rebalanceImmediately
```

实现要求：

- 如果 downstream channel 不存在，proxy 应返回错误并触发该 clientId 的上游注销。
- 对 `NOTIFY_CONSUMER_IDS_CHANGED` 这类原生客户端返回 `null` 的 oneway-like 请求，proxy 转发时要正确处理无响应或空响应。
- 不建议 proxy 本地根据成员数量变化主动广播 rebalance。标准触发源应为 broker。

### 4.5 GET_CONSUMER_LIST_BY_GROUP

这是多 proxy 下最关键的请求。consumer rebalance 时会调用该请求获取 group 全局成员。

当前本地返回方式：

```text
proxy: 遍历本地 ClientConnectionManager，只返回当前 proxy 上的 consumerId
```

该方式只适合单 proxy。多 proxy 下会导致每个 proxy 上的 consumer 只看到本地成员，最终重复分配同一批队列。

推荐方式：

```text
consumer -> proxy: GET_CONSUMER_LIST_BY_GROUP(group)
proxy -> broker: GET_CONSUMER_LIST_BY_GROUP(group)
broker -> proxy: 全局 consumerIdList
proxy -> consumer: 原样返回
```

实现要求：

- 按请求里的 topic 或当前 route 选择一个真实 broker。
- 如果无法选择 broker，可以从 `VirtualRouteManager.getAllRealBrokerAddrs()` 选择可用 broker。
- 本地 consumer list 只能作为调试或降级兜底，不能作为标准路径。

### 4.6 LOCK_BATCH_MQ / UNLOCK_BATCH_MQ

顺序消费依赖 broker 侧队列锁。proxy 必须按真实 broker 语义转发。

要求：

- 请求体里的 `clientId` 必须保持真实 consumer clientId。
- `MessageQueue.brokerName` 保持 brokerName 不变，proxy 只做 brokerAddr 虚拟化。
- 根据 brokerName 或 queue route 转发到真实 broker。
- 不要在 proxy 本地实现队列锁。

## 5. 当前代码差距与改造建议

### 5.1 ClientConnectionManager

当前能力：

- 维护 `channelClientMap` 和 `clientIdChannelMap`。
- 保存 consumer group、subscription、messageModel 等信息。
- 提供本地 group 查询能力。

建议改造：

- 增加按 clientId 查询 `ClientInfo` 的公开方法，避免其他类遍历全量列表。
- `onChannelInactive` 返回或回调下线前的 `ClientInfo`，便于上游注销。
- 明确该类只负责下游会话，不负责全局 group membership。

### 5.2 ClientManageProcessor

当前能力：

- 能解析下游 `HEART_BEAT` 并注册本地 consumer。
- 能处理 `UNREGISTER_CLIENT` 并清理本地状态。
- 能处理 `NOTIFY_CONSUMER_IDS_CHANGED` 并转发给本地 group 成员。

主要问题：

- 本地根据 `consumerGroupMemberCount` 触发 `notifyGroupMembers`，只适合单 proxy。
- `GET_CONSUMER_LIST_BY_GROUP` 返回本地成员，不满足多 proxy 全局视图。
- `UNREGISTER_CLIENT` 只清理本地状态，没有保证同步 broker。

建议改造：

- 移除或降级本地 member count 触发 rebalance 的逻辑。
- 新增上游会话管理依赖，例如 `UpstreamConsumerSessionManager`。
- `HEART_BEAT` 后立即调用上游心跳同步。
- `UNREGISTER_CLIENT` 后调用上游注销。
- `GET_CONSUMER_LIST_BY_GROUP` 标准路径改为转发 broker。
- `NOTIFY_CONSUMER_IDS_CHANGED` 只处理来自 broker 的通知转发，不递归触发本地 rebalance 决策。

### 5.3 ProxyBrokerHeartbeatService

当前能力：

- 周期性把本地 client 信息作为 heartbeat 发送给 broker。
- 按 clientId 创建 dedicated `NettyRemotingClient`。
- 注册 `NOTIFY_CONSUMER_IDS_CHANGED`、`GET_CONSUMER_RUNNING_INFO` 等 broker 反向处理器。
- 能把 broker 反向请求转发到真实 consumer channel。

主要问题：

- 职责已超过 heartbeat，建议拆成上游会话管理器。
- 当前主要依赖周期性 heartbeat，首次上线和下线收敛不够及时。
- 清理 stale client 时应优先显式发送 `UNREGISTER_CLIENT`。
- 反向转发失败时应触发上游注销，避免 broker 长时间保留幽灵成员。

建议改造：

- 保留定时 heartbeat 作为兜底。
- 新增事件驱动方法：
  - `syncHeartbeat(ClientInfo clientInfo)`
  - `unregisterClient(ClientInfo clientInfo)`
  - `closeUpstreamSession(String clientId)`
  - `forwardBrokerRequest(String clientId, RemotingCommand request)`
- 将 `BrokerToClientForwardProcessor` 从内部类演进为可测试的独立类。

### 5.4 NettyRemotingClient

当前 `registerProcessor` 能支持 broker 反向请求。使用上需要明确：

- consumer 上游连接不能简单当作普通 broker 连接池。
- 对 broker 来说，每个 upstream channel 都代表某个真实 clientId。
- producer 与 consumer 不应无脑共用同一上游连接模型。
- 转发请求时要保持 `opaque`、`flag`、`serializeType` 等 remoting 语义。

## 6. 推荐新增职责对象

建议新增或逻辑上抽象出 `UpstreamConsumerSessionManager`。

```java
public interface UpstreamConsumerSessionManager {
    void syncHeartbeat(ClientConnectionManager.ClientInfo clientInfo);

    void unregisterClient(ClientConnectionManager.ClientInfo clientInfo);

    RemotingCommand getConsumerListByGroup(String consumerGroup, String topic);

    void closeSession(String clientId);
}
```

核心职责：

- 维护 `clientId -> NettyRemotingClient`。
- 根据 route 找到真实 broker 地址。
- 对 broker 发送 `HEART_BEAT`、`UNREGISTER_CLIENT`、`GET_CONSUMER_LIST_BY_GROUP`。
- 注册 broker 反向 request processor。
- 将 broker 反向 request 转给下游 consumer。

这样 `ClientManageProcessor` 不需要理解上游连接池细节，只负责协议入口和本地下游状态。

## 7. 需要避免的错误设计

### 7.1 proxy 本地充当 rebalance 权威

不推荐：

```text
proxy 本地发现 group 成员变化
  -> proxy 直接通知本地所有 consumer rebalance
```

原因：

- 多 proxy 下只能看到局部成员。
- rebalance 后 consumer 拉取的 consumer list 如果仍是局部列表，会重复分配队列。
- broker 的 filter、subscription、lock 等状态可能没有同步完成。

### 7.2 多个真实 consumer 共用一个上游 consumer channel

不推荐：

```text
consumerA, consumerB -> proxy -> 同一个 proxy->broker channel
```

原因：

- broker 的 `ConsumerManager` 按 group 下的 channel/clientId 维护成员。
- broker 反向通知是发给 channel 的。
- 多个真实 consumer 共用 channel 会让反向请求路由、下线清理和成员更新变复杂。

推荐按真实 clientId 维度维护独立上游会话。

### 7.3 `GET_CONSUMER_LIST_BY_GROUP` 返回本地成员

这会直接破坏多 proxy 下的队列分配，是最需要优先修复的协议缺口。

## 8. 测试矩阵

### 8.1 单 proxy 基础测试

| 场景 | 预期 |
|------|------|
| 一个 consumer 启动 | broker 能看到真实 clientId，consumer 能正常消费 |
| 两个 consumer 同 group 启动 | broker 返回两个 consumerId，队列分配不重复 |
| 一个 consumer 主动 shutdown | proxy 向 broker 注销，剩余 consumer 收到 rebalance |
| 一个 consumer 异常断线 | proxy 主动注销 broker，剩余 consumer 收到 rebalance |
| subscription 变化 | broker 收到新 subscription，触发必要通知 |

### 8.2 多 proxy 测试

| 场景 | 预期 |
|------|------|
| consumerA 连 proxy1，consumerB 连 proxy2，同 group | 两个 consumer 获取同一份全局 consumerIdList |
| proxy1 上 consumer 下线 | proxy2 上 consumer 收到 broker 触发的 rebalance |
| proxy2 上 consumer 新增 | proxy1 上 consumer 收到 broker 触发的 rebalance |
| `GET_CONSUMER_LIST_BY_GROUP` 经任意 proxy 发起 | 返回 broker 全局成员，不是本地成员 |
| proxy1 整体宕机 | broker 通过 channel close/scan 清理成员，proxy2 consumer 最终 rebalance |

### 8.3 顺序消费测试

| 场景 | 预期 |
|------|------|
| 两个顺序 consumer 同 group | `LOCK_BATCH_MQ` 由 broker 决策，队列不会被双重锁定 |
| consumer 下线释放队列 | `UNLOCK_BATCH_MQ` 或 channel 清理后，其他 consumer 可重新锁定 |
| 多 proxy 顺序消费 | 锁语义仍以 broker 为准 |

### 8.4 反向请求测试

| 场景 | 预期 |
|------|------|
| broker 发送 `NOTIFY_CONSUMER_IDS_CHANGED` | proxy 转发给对应真实 consumer |
| broker 发送 `GET_CONSUMER_RUNNING_INFO` | proxy 转发并返回真实 consumer 响应 |
| downstream 已断开时 broker 发反向请求 | proxy 返回错误并清理该 clientId 上游会话 |

### 8.5 兼容性测试

| 场景 | 预期 |
|------|------|
| 原生 `DefaultMQPushConsumer` | 无需修改代码即可经 proxy 消费 |
| 原生 `DefaultLitePullConsumer` | rebalance 和 offset 正常 |
| 广播模式 consumer | 不依赖 clustering consumer list 分配 |
| Tag 订阅 | subscription 同步到 broker，过滤结果正确 |

## 9. 推荐实施顺序

1. 修复 `GET_CONSUMER_LIST_BY_GROUP`，改为 broker 全局转发。
2. 为 `UNREGISTER_CLIENT` 和 downstream `channelInactive` 增加上游显式注销。
3. 把首次 `HEART_BEAT` 后的上游同步改为事件驱动，保留定时 heartbeat 兜底。
4. 把 `ProxyBrokerHeartbeatService` 拆分或重命名为上游 consumer 会话管理职责。
5. 完善 broker 反向请求转发，至少覆盖：
   - `NOTIFY_CONSUMER_IDS_CHANGED`
   - `GET_CONSUMER_RUNNING_INFO`
   - 后续按需支持 `RESET_CONSUMER_CLIENT_OFFSET`、`GET_CONSUMER_STATUS_FROM_CLIENT`、`CONSUME_MESSAGE_DIRECTLY`
6. 补齐单 proxy、多 proxy、顺序消费和异常断线测试。

## 10. 最终判断

消费者代理的关键不是“proxy 发 rebalance”，而是让 RocketMQ 原生的 rebalance 触发和计算链路完整穿过 proxy：

```text
broker 维护全局成员
broker 通知 proxy 上游 channel
proxy 转发通知到真实 consumer
consumer 触发原生 rebalance
consumer 通过 proxy 从 broker 获取全局 consumer list
consumer 本地计算队列分配
```

只要坚持 broker 是 group membership 权威、consumer 是 rebalance 计算权威、proxy 是双向连接和协议转发层，多 proxy 下的消费者上线、下线、队列变化和顺序消费才能保持与 RocketMQ 原生行为一致。
