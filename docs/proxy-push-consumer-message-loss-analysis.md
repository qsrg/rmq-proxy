# ProxyPushConsumer 消息丢失问题分析

## 问题概述

ProxyPushConsumer 存在消息丢失风险：offset 在消息消费完成前就被推进，且消费失败时（RECONSUME_LATER）没有任何重试处理机制。

## 问题代码

`mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/consumer/push/ProxyPushConsumer.java`

### 问题1：offset 在消费完成前推进

PullTask.run() 中，消息异步投递到 consumeExecutor 后立即推进 offset：

```java
// 行 460-463
dispatchToListener(proxyMessages);                    // 异步投递，不等待消费完成
offsetTable.put(mq, result.getNextBeginOffset());     // 立即推进 offset
```

**后果**：如果消费者进程在消息被实际消费前崩溃，这些消息的 offset 已经提交，重启后不会重新拉取，导致消息永久丢失。

### 问题2：ConsumeStatus 返回值被忽略

```java
// 行 397-407
private void dispatchToListener(List<ProxyMessage> messages) {
    consumeExecutor.execute(() -> {
        try {
            if (messageListener != null) {
                messageListener.consume(messages);  // 返回 ConsumeStatus 被完全忽略
            }
        } catch (Exception e) {
            log.error("Message listener error", e);
        }
    });
}
```

**后果**：当 listener 返回 `RECONSUME_LATER` 时，offset 仍然被无条件推进，消费失败的消息直接丢失。

## RocketMQ 原生实现对比

### 核心机制差异

| 机制 | RocketMQ 原生 | ProxyPushConsumer |
|------|--------------|-------------------|
| 消息跟踪 | ProcessQueue (`TreeMap<offset, MessageExt>`) 跟踪每条消息消费状态 | 无，消息投递后无法追踪 |
| Offset 推进时机 | 消费完成后，`removeMessage()` 返回正确的待提交 offset | 拉取后立即推进，不等消费完成 |
| RECONSUME_LATER 处理 | `sendMessageBack()` 发回 Broker 重试队列 `%RETRY%+Group` | 完全忽略，消息直接丢失 |
| 重试机制 | 最多16次重试，超过进死信队列 `%DLQ%+Group` | 无 |
| Offset 计算 | `removeMessage()` 返回 `msgTreeMap.firstKey()`（剩余消息的最小 offset） | 直接用 `nextBeginOffset` |

### RocketMQ 原生流程

```
拉取消息 → 放入 ProcessQueue → 提交消费 → 消费完成 → 处理结果 → 更新 offset
                                                    ↓
                                              RECONSUME_LATER?
                                              → sendMessageBack 发回重试队列
                                              → 或本地延迟重试
```

关键源码位置（RocketMQ 4.9.8）：

1. **拉取消息时** — 消息进入 ProcessQueue，offset 不推进
   - `client/.../DefaultMQPushConsumerImpl.java` 行 316-349
   - `processQueue.putMessage(msgFoundList)` + `submitConsumeRequest()`
   - 只更新 `pullRequest.nextOffset`（下次拉取位置），不更新消费 offset

2. **消费完成后** — 才更新 offset
   - `client/.../ConsumeMessageConcurrentlyService.java` 行 241-302
   - `processConsumeResult()` 方法：
     - 根据 status 确定 ackIndex（SUCCESS=全部成功，LATER=全部失败）
     - CLUSTERING 模式下失败消息调用 `sendMessageBack()` 发回 Broker 重试队列
     - 调用 `processQueue.removeMessage(msgs)` 获取正确的待提交 offset
     - 调用 `offsetStore.updateOffset(mq, offset, true)` 更新 offset

3. **RECONSUME_LATER 时 offset 也会推进，但不丢消息**
   - 失败消息通过 `sendMessageBack` 发到重试 Topic `%RETRY%+ConsumerGroup`
   - 消费者自动订阅重试 Topic，从重试队列中重新消费
   - 原始队列的 offset 可以安全推进

4. **sendMessageBack 失败的保护**
   - 发回失败的消息从当前批次移除，5秒后重新提交消费
   - `removeMessage()` 返回 `msgTreeMap.firstKey()`，确保 offset 不会跳过未消费消息

## 修复方案

### 方案一：引入 ProcessQueue 机制（推荐，与 RocketMQ 对齐）

分步实施：

| 步骤 | 改动 | 优先级 |
|------|------|--------|
| Step 1 | 引入 ProcessQueue，用 `TreeMap<Long, ProxyMessage>` 跟踪每条消息消费状态 | P0 |
| Step 2 | offset 改为消费完成后提交，`removeMessage()` 返回正确的待提交 offset | P0 |
| Step 3 | 处理 ConsumeStatus 返回值，RECONSUME_LATER 时不推进 offset | P0 |
| Step 4 | 实现 sendMessageBack，将失败消息发回 Broker 重试队列 | P1 |
| Step 5 | 拉取与消费解耦，拉取只更新 nextPullOffset | P1 |
| Step 6 | 添加最大重试次数和死信队列支持 | P2 |

核心改动：
- 新增 ProcessQueue 类，管理每个 MessageQueue 的消息缓存和消费跟踪
- PullTask.run() 中：拉取消息 → 放入 ProcessQueue → 提交消费请求
- 消费完成后：处理 ConsumeStatus → 处理失败消息 → removeMessage → 更新 offset
- 新增 sendMessageBack 方法，通过 CONSUMER_SEND_MSG_BACK 请求码将失败消息发回 Broker

### 方案二：简化方案（最小改动）

1. 同步消费 — 在 PullTask 中同步调用 listener，等待消费结果
2. 根据消费结果决定是否推进 offset — RECONSUME_LATER 时不推进
3. 实现 sendMessageBack — 将失败消息发回 Broker 重试队列

缺点：同步消费会阻塞拉取线程，降低吞吐量。

## 影响范围

- 所有使用 ProxyPushConsumer 的消费者
- 集群模式（CLUSTERING）下影响最大，消息丢失不可恢复
- 广播模式（BROADCASTING）下影响较小（RocketMQ 原生广播模式也不重试）

## 状态

- 发现日期：2026-06-03
- 状态：待修复
- 优先级：P0
