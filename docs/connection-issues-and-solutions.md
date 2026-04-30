# MQ-Proxy 连接架构问题与解决方案

## 一、连接架构全景

```
┌─────────────────────────────────────────────────────────────────┐
│                      RocketMQ 原生客户端                         │
│  (DefaultMQProducer / DefaultMQPushConsumer)                    │
│                                                                 │
│  内置 MQClientInstance:                                         │
│   - 每30秒发送心跳 (HEART_BEAT)                                  │
│   - shutdown时发送 UNREGISTER_CLIENT                             │
│   - NettyRemotingClient: 长连接 + IdleStateHandler(120s)         │
└──────────────┬──────────────────────────────────────────────────┘
               │ ① TCP长连接 (SO_KEEPALIVE=true)
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                         MQ-Proxy                                │
│                                                                 │
│  NettyRemotingServer (面向客户端)                                │
│   - SO_KEEPALIVE=true                                           │
│   - IdleStateHandler(120s) ← 修复后新增                         │
│   - channelInactive 清理本地客户端注册 ← 修复后新增               │
│   - ClientManageProcessor: 本地维护客户端注册表                   │
│                                                                 │
│  ProxyBrokerHeartbeatService ← 修复后新增                        │
│   - 定期向Broker发送Proxy自身心跳                                │
│   - 汇总所有在线客户端的group信息                                │
│                                                                 │
│  NettyRemotingClient (面向Broker)                               │
│   - RocketMQStorageAdapter持有                                   │
│   - 按addr缓存Channel，长连接复用                                │
└──────────────┬──────────────────────────────────────────────────┘
               │ ② TCP长连接 (共享复用)
               ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Broker (RocketMQ 4.9.8)                    │
│                                                                 │
│  NettyRemotingServer:                                           │
│   - IdleStateHandler(120s ALL_IDLE)                             │
│   - ClientHousekeepingService: 每10秒扫描不活跃Channel            │
│   - CHANNEL_EXPIRED_TIMEOUT = 120秒                              │
│                                                                 │
│  ProducerManager / ConsumerManager:                              │
│   - 心跳更新 lastUpdateTimestamp                                 │
│   - 120秒无更新 → 清理Channel → 触发Rebalance                    │
└─────────────────────────────────────────────────────────────────┘
```

## 二、发现的问题

### 问题1：心跳转发导致 Broker 端 Channel 身份混淆（严重）

**现象**：多个客户端通过 Proxy 向 Broker 发送心跳时，Broker 端所有客户端的注册信息都关联到同一个 Channel（Proxy 的 Channel）。

**根因**：`ClientManageProcessor.heartBeat()` 直接调用 `storageAdapter.forwardToBroker(request)` 转发心跳。Broker 收到后用 `ctx.channel()`（即 Proxy 的 Channel）创建 `ClientChannelInfo`，导致：
- 同组消费者互相覆盖
- 注销一个消费者可能影响其他消费者
- Proxy Channel 断开时所有客户端注册信息被批量清理

### 问题2：Proxy 端无连接生命周期管理（严重）

**现象**：客户端异常断开（进程崩溃、网络中断）后，Proxy 无法感知，本地客户端注册表永远残留。

**根因**：
- `NettyRemotingServer` pipeline 中没有 `IdleStateHandler`
- `channelInactive` 回调只打印日志，无清理逻辑
- 没有定时扫描不活跃 Channel 的机制

### 问题3：Proxy → Broker 无主动心跳（中等）

**现象**：所有客户端断开后，Proxy → Broker 的连接在120秒后被 Broker 关闭，存在故障窗口。

**根因**：Proxy 自身不向 Broker 发送心跳，完全依赖客户端心跳的转发。无客户端时无心跳。

### 问题4：UNREGISTER_CLIENT 转发时 Channel 不匹配（中等）

**现象**：多客户端场景下，注销请求转发到 Broker 后可能产生副作用。

**根因**：与问题1同源，所有客户端共享 Proxy 的 Channel 注册。

### 问题5：NettyRemotingServer 中多余的客户端连接代码（轻微）

**现象**：`NettyRemotingServer.getAndCreateChannel()` 让 Server 也能作为 Client 发起连接，在 Proxy 架构中多余。

## 三、解决方案

### 修复1：Proxy 自主管理客户端注册（解决问题1、4）

**核心思路**：Proxy 不再简单转发心跳和注销请求，而是：
1. 本地维护完整的客户端连接注册表（Channel → ClientID → Groups）
2. Proxy 以自身身份向 Broker 发送合并后的心跳
3. 客户端注销时，Proxy 本地清理后，以 Proxy 身份向 Broker 发送更新后的心跳

**改动文件**：
- `ClientManageProcessor.java`：重写心跳和注销逻辑
- 新增 `ClientConnectionManager.java`：统一管理客户端连接注册

### 修复2：添加连接生命周期管理（解决问题2）

**核心思路**：
1. `NettyRemotingServer` pipeline 添加 `IdleStateHandler(120, 120, 120)`
2. `channelInactive` 回调中清理本地注册表并通知 Broker
3. 添加定时扫描不活跃 Channel 的服务

**改动文件**：
- `NettyRemotingServer.java`：添加 IdleStateHandler、完善 channelInactive
- `ClientConnectionManager.java`：提供连接清理接口

### 修复3：添加 Proxy → Broker 主动心跳（解决问题3）

**核心思路**：新增 `ProxyBrokerHeartbeatService`，定期汇总所有在线客户端信息，以 Proxy 身份向 Broker 发送心跳。

**改动文件**：
- 新增 `ProxyBrokerHeartbeatService.java`
- `RocketMQStorageAdapter.java`：添加发送心跳方法
- `ProxyStartup.java`：启动心跳服务

### 修复4：清理多余代码（解决问题5）

**改动文件**：
- `NettyRemotingServer.java`：移除 `getAndCreateChannel`、`invokeSync`、`invokeOneway` 等客户端方法

## 四、修复后的连接生命周期

```
客户端启动
  │
  ├─ 客户端连接 Proxy (TCP长连接, SO_KEEPALIVE=true)
  │
  ├─ 客户端发送 HEART_BEAT
  │     → Proxy ClientManageProcessor 收到
  │       ├─ 注册到本地 ClientConnectionManager
  │       └─ 不再直接转发，由 ProxyBrokerHeartbeatService 统一处理
  │
  ├─ ProxyBrokerHeartbeatService (每30秒)
  │     → 汇总所有在线客户端的 group 信息
  │     → 以 Proxy 自身身份向 Broker 发送合并心跳
  │     → Broker 端只看到 Proxy 一个 Channel，注册信息正确
  │
  ├─ 客户端 shutdown
  │     → 发送 UNREGISTER_CLIENT
  │       ├─ Proxy 本地清理该客户端注册
  │       └─ 下次心跳周期自动更新 Broker 端注册
  │     → 客户端关闭连接
  │       ├─ Proxy channelInactive 触发
  │       ├─ 清理本地注册表
  │       └─ 下次心跳周期自动更新 Broker 端注册
  │
  ├─ 客户端异常断开
  │     → Proxy IdleStateHandler 120秒超时
  │       ├─ 触发 channelInactive
  │       ├─ 清理本地注册表
  │       └─ 下次心跳周期自动更新 Broker 端注册
  │
  └─ Proxy → Broker 连接
        → ProxyBrokerHeartbeatService 持续发送心跳
        → 即使无客户端，Proxy 也保持与 Broker 的连接活跃
```
