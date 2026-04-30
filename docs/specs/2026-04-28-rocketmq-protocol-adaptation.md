# RocketMQ 4.9.8 协议适配清单

## 1. 协议帧结构

### 1.1 RemotingCommand编码格式

```
+----------------+------------------+------------------+------------------+
|  Total Length  | Header Length    |   Header Data    |    Body Data     |
|   (4 bytes)    | (4 bytes,含类型) |    (变长)        |     (变长)        |
+----------------+------------------+------------------+------------------+

- Total Length (4字节): 整个帧长度（不含自身）
- Header Length (4字节): 高8位存储序列化类型，低24位存储header长度
- 序列化类型: JSON(0) 或 ROCKETMQ(1)
- Header Data: RemotingCommand序列化后的数据
- Body Data: 消息业务数据
```

### 1.2 RemotingCommand核心字段

| 字段 | 类型 | 说明 |
|------|------|------|
| code | int | 请求码/响应码 |
| language | LanguageCode | 语言代码，默认JAVA |
| version | int | 协议版本号 |
| opaque | int | 请求唯一标识（自增ID），请求响应匹配 |
| flag | int | 标志位（bit0=RPC类型0请求1响应，bit1=是否单向） |
| remark | String | 备注/错误信息 |
| extFields | HashMap<String,String> | 扩展字段，承载业务参数 |
| customHeader | CommandCustomHeader | 自定义头部（业务Header） |
| body | byte[] | 消息体数据 |
| serializeTypeCurrentRPC | SerializeType | 序列化类型（JSON/ROCKETMQ） |

### 1.3 序列化类型

| 类型 | 值 | 说明 |
|------|-----|------|
| JSON | 0 | JSON序列化，调试友好 |
| ROCKETMQ | 1 | RocketMQ自定义序列化，更高效 |

---

## 2. 全部请求码（RequestCode）

### 2.1 消息收发类（核心链路）

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| SEND_MESSAGE | 10 | 单条消息发送V1 | SendMessageProcessor | SendMessageRequestHeader |
| PULL_MESSAGE | 11 | 消息拉取 | PullMessageProcessor | PullMessageRequestHeader |
| QUERY_MESSAGE | 12 | 查询消息 | QueryMessageProcessor | QueryMessageRequestHeader |
| QUERY_BROKER_OFFSET | 13 | 查询Broker最大偏移量 | AdminBrokerProcessor | - |
| QUERY_CONSUMER_OFFSET | 14 | 查询消费者偏移量 | ConsumerManageProcessor | QueryConsumerOffsetRequestHeader |
| UPDATE_CONSUMER_OFFSET | 15 | 更新消费者偏移量 | ConsumerManageProcessor | UpdateConsumerOffsetRequestHeader |
| CONSUMER_SEND_MSG_BACK | 36 | 消息消费失败发回 | SendMessageProcessor | ConsumerSendMsgBackRequestHeader |
| END_TRANSACTION | 37 | 结束事务 | EndTransactionProcessor | EndTransactionRequestHeader |
| SEND_MESSAGE_V2 | 310 | 发送消息V2（优化版） | SendMessageProcessor | SendMessageRequestHeaderV2 |
| SEND_BATCH_MESSAGE | 320 | 批量发送消息 | SendMessageProcessor | SendMessageRequestHeaderV2 |
| SEND_REPLY_MESSAGE | 324 | 发送回复消息 | ReplyMessageProcessor | ReplyMessageRequestHeader |
| SEND_REPLY_MESSAGE_V2 | 325 | 发送回复消息V2 | ReplyMessageProcessor | ReplyMessageRequestHeader |
| VIEW_MESSAGE_BY_ID | 33 | 按ID查看消息 | QueryMessageProcessor | ViewMessageRequestHeader |

### 2.2 客户端管理类

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| HEART_BEAT | 34 | 客户端心跳 | ClientManageProcessor | (body: HeartbeatData) |
| UNREGISTER_CLIENT | 35 | 客户端注销 | ClientManageProcessor | UnregisterClientRequestHeader |
| CHECK_CLIENT_CONFIG | 46 | 检查客户端配置 | ClientManageProcessor | - |
| GET_CONSUMER_LIST_BY_GROUP | 38 | 按组获取消费者列表 | ConsumerManageProcessor | GetConsumerListByGroupRequestHeader |
| LOCK_BATCH_MQ | 41 | 批量锁定队列 | AdminBrokerProcessor | - |
| UNLOCK_BATCH_MQ | 42 | 批量解锁队列 | AdminBrokerProcessor | - |

### 2.3 Topic与配置管理类

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| UPDATE_AND_CREATE_TOPIC | 17 | 创建/更新Topic | AdminBrokerProcessor | CreateTopicRequestHeader |
| GET_ALL_TOPIC_CONFIG | 21 | 获取所有Topic配置 | AdminBrokerProcessor | - |
| GET_TOPIC_CONFIG_LIST | 22 | 获取Topic配置列表 | AdminBrokerProcessor | - |
| GET_TOPIC_NAME_LIST | 23 | 获取Topic名称列表 | AdminBrokerProcessor | - |
| DELETE_TOPIC_IN_BROKER | 215 | 删除Broker上的Topic | AdminBrokerProcessor | DeleteTopicRequestHeader |
| GET_SYSTEM_TOPIC_LIST_FROM_BROKER | 305 | 获取系统Topic列表 | AdminBrokerProcessor | - |
| CLEAN_UNUSED_TOPIC | 316 | 清理未使用Topic | AdminBrokerProcessor | - |

### 2.4 Broker运维管理类

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| UPDATE_BROKER_CONFIG | 25 | 更新Broker配置 | AdminBrokerProcessor | - |
| GET_BROKER_CONFIG | 26 | 获取Broker配置 | AdminBrokerProcessor | - |
| TRIGGER_DELETE_FILES | 27 | 触发删除文件 | AdminBrokerProcessor | - |
| GET_BROKER_RUNTIME_INFO | 28 | 获取Broker运行信息 | AdminBrokerProcessor | - |
| SEARCH_OFFSET_BY_TIMESTAMP | 29 | 按时间戳搜索偏移量 | AdminBrokerProcessor | SearchOffsetRequestHeader |
| GET_MAX_OFFSET | 30 | 获取最大偏移量 | AdminBrokerProcessor | GetMaxOffsetRequestHeader |
| GET_MIN_OFFSET | 31 | 获取最小偏移量 | AdminBrokerProcessor | GetMinOffsetRequestHeader |
| GET_EARLIEST_MSG_STORETIME | 32 | 获取最早消息存储时间 | AdminBrokerProcessor | GetEarliestMsgStoretimeRequestHeader |
| GET_ALL_CONSUMER_OFFSET | 43 | 获取所有消费者偏移量 | AdminBrokerProcessor | - |
| GET_ALL_DELAY_OFFSET | 45 | 获取所有延迟偏移量 | AdminBrokerProcessor | - |
| DELETE_EXPIRED_COMMITLOG | 329 | 删除过期CommitLog | AdminBrokerProcessor | - |

### 2.5 消费者管理类

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| UPDATE_AND_CREATE_SUBSCRIPTIONGROUP | 200 | 创建/更新订阅组 | AdminBrokerProcessor | - |
| GET_ALL_SUBSCRIPTIONGROUP_CONFIG | 201 | 获取所有订阅组配置 | AdminBrokerProcessor | - |
| DELETE_SUBSCRIPTIONGROUP | 207 | 删除订阅组 | AdminBrokerProcessor | DeleteSubscriptionGroupRequestHeader |
| GET_CONSUMER_CONNECTION_LIST | 203 | 获取消费者连接列表 | AdminBrokerProcessor | GetConsumerConnectionListRequestHeader |
| GET_PRODUCER_CONNECTION_LIST | 204 | 获取生产者连接列表 | AdminBrokerProcessor | GetProducerConnectionListRequestHeader |
| GET_CONSUME_STATS | 208 | 获取消费统计 | AdminBrokerProcessor | GetConsumeStatsRequestHeader |
| SUSPEND_CONSUMER | 209 | 暂停消费者 | AdminBrokerProcessor | - |
| RESUME_CONSUMER | 210 | 恢复消费者 | AdminBrokerProcessor | - |
| RESET_CONSUMER_OFFSET_IN_CONSUMER | 211 | 重置消费者偏移量(消费者侧) | AdminBrokerProcessor | - |
| RESET_CONSUMER_OFFSET_IN_BROKER | 212 | 重置消费者偏移量(Broker侧) | AdminBrokerProcessor | - |
| ADJUST_CONSUMER_THREAD_POOL | 213 | 调整消费者线程池 | AdminBrokerProcessor | - |
| WHO_CONSUME_THE_MESSAGE | 214 | 查询谁消费了消息 | AdminBrokerProcessor | - |
| INVOKE_BROKER_TO_RESET_OFFSET | 222 | 通知Broker重置偏移量 | AdminBrokerProcessor | ResetOffsetRequestHeader |
| INVOKE_BROKER_TO_GET_CONSUMER_STATUS | 223 | 获取消费者状态 | AdminBrokerProcessor | GetConsumerStatusRequestHeader |
| CLONE_GROUP_OFFSET | 314 | 克隆组偏移量 | AdminBrokerProcessor | CloneGroupOffsetRequestHeader |
| GET_BROKER_CONSUME_STATS | 317 | 获取Broker消费统计 | AdminBrokerProcessor | GetConsumeStatsInBrokerHeader |

### 2.6 查询与诊断类

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| GET_TOPIC_STATS_INFO | 202 | 获取Topic统计信息 | AdminBrokerProcessor | GetTopicStatsInfoRequestHeader |
| QUERY_TOPIC_CONSUME_BY_WHO | 300 | 查询Topic被谁消费 | AdminBrokerProcessor | QueryTopicConsumeByWhoRequestHeader |
| QUERY_CONSUME_TIME_SPAN | 303 | 查询消费时间跨度 | AdminBrokerProcessor | QueryConsumeTimeSpanRequestHeader |
| GET_CONSUMER_RUNNING_INFO | 307 | 获取消费者运行信息 | AdminBrokerProcessor | GetConsumerRunningInfoRequestHeader |
| QUERY_CORRECTION_OFFSET | 308 | 查询修正偏移量 | AdminBrokerProcessor | QueryCorrectionOffsetHeader |
| CONSUME_MESSAGE_DIRECTLY | 309 | 直接消费消息 | AdminBrokerProcessor | ConsumeMessageDirectlyResultRequestHeader |
| VIEW_BROKER_STATS_DATA | 315 | 查看Broker统计数据 | AdminBrokerProcessor | ViewBrokerStatsDataRequestHeader |
| QUERY_CONSUME_QUEUE | 321 | 查询消费队列 | AdminBrokerProcessor | QueryConsumeQueueRequestHeader |
| GET_ALL_PRODUCER_INFO | 328 | 获取所有生产者信息 | AdminBrokerProcessor | GetAllProducerInfoRequestHeader |

### 2.7 ACL权限管理类

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| UPDATE_AND_CREATE_ACL_CONFIG | 50 | 创建/更新ACL配置 | AdminBrokerProcessor | CreateAccessConfigRequestHeader |
| DELETE_ACL_CONFIG | 51 | 删除ACL配置 | AdminBrokerProcessor | DeleteAccessConfigRequestHeader |
| GET_BROKER_CLUSTER_ACL_INFO | 52 | 获取Broker集群ACL信息 | AdminBrokerProcessor | - |
| UPDATE_GLOBAL_WHITE_ADDRS_CONFIG | 53 | 更新全局白名单 | AdminBrokerProcessor | UpdateGlobalWhiteAddrsConfigRequestHeader |

### 2.8 NameServer协议类

| 请求码 | 值 | 说明 | NameServer处理器 | Header类 |
|--------|-----|------|-----------------|---------|
| PUT_KV_CONFIG | 100 | 存储KV配置 | DefaultRequestProcessor | - |
| GET_KV_CONFIG | 101 | 获取KV配置 | DefaultRequestProcessor | - |
| DELETE_KV_CONFIG | 102 | 删除KV配置 | DefaultRequestProcessor | - |
| REGISTER_BROKER | 103 | Broker注册 | DefaultRequestProcessor | RegisterBrokerRequestHeader |
| UNREGISTER_BROKER | 104 | Broker注销 | DefaultRequestProcessor | UnregisterBrokerRequestHeader |
| GET_ROUTEINFO_BY_TOPIC | 105 | 获取Topic路由信息 | DefaultRequestProcessor | GetRouteInfoRequestHeader |
| GET_BROKER_CLUSTER_INFO | 106 | 获取Broker集群信息 | DefaultRequestProcessor | - |
| WIPE_WRITE_PERM_OF_BROKER | 205 | 清除Broker写权限 | DefaultRequestProcessor | - |
| GET_ALL_TOPIC_LIST_FROM_NAMESERVER | 206 | 获取所有Topic列表 | DefaultRequestProcessor | - |
| DELETE_TOPIC_IN_NAMESRV | 216 | 删除NameServer上的Topic | DefaultRequestProcessor | DeleteTopicRequestHeader |
| GET_KVLIST_BY_NAMESPACE | 219 | 按命名空间获取KV列表 | DefaultRequestProcessor | - |
| GET_TOPICS_BY_CLUSTER | 224 | 按集群获取Topic列表 | DefaultRequestProcessor | GetTopicsByClusterRequestHeader |
| GET_SYSTEM_TOPIC_LIST_FROM_NS | 304 | 获取系统Topic列表 | DefaultRequestProcessor | - |
| GET_UNIT_TOPIC_LIST | 311 | 获取单元Topic列表 | DefaultRequestProcessor | - |
| GET_HAS_UNIT_SUB_TOPIC_LIST | 312 | 获取含单元子Topic列表 | DefaultRequestProcessor | - |
| GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST | 313 | 获取含单元子非单元Topic列表 | DefaultRequestProcessor | - |
| UPDATE_NAMESRV_CONFIG | 318 | 更新NameServer配置 | DefaultRequestProcessor | - |
| GET_NAMESRV_CONFIG | 319 | 获取NameServer配置 | DefaultRequestProcessor | - |
| QUERY_DATA_VERSION | 322 | 查询数据版本 | DefaultRequestProcessor | - |
| ADD_WRITE_PERM_OF_BROKER | 327 | 增加Broker写权限 | DefaultRequestProcessor | - |

### 2.9 客户端回调类（Broker→Client）

| 请求码 | 值 | 说明 | 客户端处理器 | Header类 |
|--------|-----|------|-------------|---------|
| CHECK_TRANSACTION_STATE | 39 | 事务状态回查 | ClientRemotingProcessor | CheckTransactionStateRequestHeader |
| NOTIFY_CONSUMER_IDS_CHANGED | 40 | 通知消费者ID变更 | ClientRemotingProcessor | NotifyConsumerIdsChangedRequestHeader |
| RESET_CONSUMER_CLIENT_OFFSET | 220 | 重置消费者客户端偏移量 | ClientRemotingProcessor | - |
| GET_CONSUMER_STATUS_FROM_CLIENT | 221 | 从客户端获取消费状态 | ClientRemotingProcessor | - |
| GET_CONSUMER_RUNNING_INFO | 307 | 获取消费者运行信息 | ClientRemotingProcessor | - |
| CONSUME_MESSAGE_DIRECTLY | 309 | 直接消费消息 | ClientRemotingProcessor | - |
| PUSH_REPLY_MESSAGE_TO_CLIENT | 326 | 推送回复消息到客户端 | ClientRemotingProcessor | - |

### 2.10 其他

| 请求码 | 值 | 说明 | Broker处理器 | Header类 |
|--------|-----|------|-------------|---------|
| REGISTER_FILTER_SERVER | 301 | 注册过滤服务器 | AdminBrokerProcessor | - |
| REGISTER_MESSAGE_FILTER_CLASS | 302 | 注册消息过滤类 | AdminBrokerProcessor | - |
| CLEAN_EXPIRED_CONSUMEQUEUE | 306 | 清理过期消费队列 | AdminBrokerProcessor | - |
| RESUME_CHECK_HALF_MESSAGE | 323 | 恢复检查半消息 | AdminBrokerProcessor | ResumeCheckHalfMessageRequestHeader |

---

## 3. 全部响应码（ResponseCode）

### 3.1 系统响应码（RemotingSysResponseCode）

| 响应码 | 值 | 说明 |
|--------|-----|------|
| SUCCESS | 0 | 成功 |
| SYSTEM_ERROR | 1 | 系统错误 |
| SYSTEM_BUSY | 2 | 系统繁忙 |
| REQUEST_CODE_NOT_SUPPORTED | 3 | 不支持的请求码 |
| TRANSACTION_FAILED | 4 | 事务失败 |

### 3.2 业务响应码（ResponseCode）

| 响应码 | 值 | 说明 |
|--------|-----|------|
| FLUSH_DISK_TIMEOUT | 10 | 刷盘超时 |
| SLAVE_NOT_AVAILABLE | 11 | 从节点不可用 |
| FLUSH_SLAVE_TIMEOUT | 12 | 同步从节点超时 |
| MESSAGE_ILLEGAL | 13 | 消息非法 |
| SERVICE_NOT_AVAILABLE | 14 | 服务不可用 |
| VERSION_NOT_SUPPORTED | 15 | 版本不支持 |
| NO_PERMISSION | 16 | 无权限 |
| TOPIC_NOT_EXIST | 17 | Topic不存在 |
| TOPIC_EXIST_ALREADY | 18 | Topic已存在 |
| PULL_NOT_FOUND | 19 | 拉取未找到消息 |
| PULL_RETRY_IMMEDIATELY | 20 | 立即重试拉取 |
| PULL_OFFSET_MOVED | 21 | 拉取偏移量移动 |
| QUERY_NOT_FOUND | 22 | 查询未找到 |
| SUBSCRIPTION_PARSE_FAILED | 23 | 订阅解析失败 |
| SUBSCRIPTION_NOT_EXIST | 24 | 订阅不存在 |
| SUBSCRIPTION_NOT_LATEST | 25 | 订阅不是最新 |
| SUBSCRIPTION_GROUP_NOT_EXIST | 26 | 订阅组不存在 |
| FILTER_DATA_NOT_EXIST | 27 | 过滤数据不存在 |
| FILTER_DATA_NOT_LATEST | 28 | 过滤数据不是最新 |
| TRANSACTION_SHOULD_COMMIT | 200 | 事务应提交 |
| TRANSACTION_SHOULD_ROLLBACK | 201 | 事务应回滚 |
| TRANSACTION_STATE_UNKNOW | 202 | 事务状态未知 |
| TRANSACTION_STATE_GROUP_WRONG | 203 | 事务状态组错误 |
| NO_BUYER_ID | 204 | 无买家ID |
| NOT_IN_CURRENT_UNIT | 205 | 不在当前单元 |
| CONSUMER_NOT_ONLINE | 206 | 消费者不在线 |
| CONSUME_MSG_TIMEOUT | 207 | 消费消息超时 |
| NO_MESSAGE | 208 | 无消息 |
| UPDATE_AND_CREATE_ACL_CONFIG_FAILED | 209 | 创建/更新ACL配置失败 |
| DELETE_ACL_CONFIG_FAILED | 210 | 删除ACL配置失败 |
| UPDATE_GLOBAL_WHITE_ADDRS_CONFIG_FAILED | 211 | 更新全局白名单失败 |
| FLOW_CONTROL | 215 | 流控 |

---

## 4. Header类字段明细

### 4.1 消息发送Header

**SendMessageRequestHeader（V1）**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| producerGroup | String | 是 | 生产者组名 |
| topic | String | 是 | Topic名称 |
| defaultTopic | String | 是 | 默认Topic |
| defaultTopicQueueNums | Integer | 是 | 默认Topic队列数 |
| queueId | Integer | 是 | 目标队列ID |
| sysFlag | Integer | 是 | 系统标志 |
| bornTimestamp | Long | 是 | 消息产生时间戳 |
| flag | Integer | 是 | 消息标志 |
| properties | String | 可选 | 消息属性 |
| reconsumeTimes | Integer | 可选 | 重试次数 |
| unitMode | boolean | 可选 | 单元模式 |
| batch | boolean | 可选 | 是否批量 |
| maxReconsumeTimes | Integer | 可选 | 最大重试次数 |

**SendMessageRequestHeaderV2（V2短字段名优化）**

| 短字段 | 原字段 | 类型 | 说明 |
|--------|--------|------|------|
| a | producerGroup | String | 生产者组名 |
| b | topic | String | Topic名称 |
| c | defaultTopic | String | 默认Topic |
| d | defaultTopicQueueNums | Integer | 默认Topic队列数 |
| e | queueId | Integer | 目标队列ID |
| f | sysFlag | Integer | 系统标志 |
| g | bornTimestamp | Long | 消息产生时间戳 |
| h | flag | Integer | 消息标志 |
| i | properties | String | 消息属性 |
| j | reconsumeTimes | Integer | 重试次数 |
| k | unitMode | boolean | 单元模式 |
| l | maxReconsumeTimes | Integer | 最大重试次数 |
| m | batch | boolean | 是否批量 |
| n | brokerName | String | Broker名称 |

**SendMessageResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| msgId | String | 是 | 消息ID |
| queueId | Integer | 是 | 队列ID |
| queueOffset | Long | 是 | 队列偏移量 |
| transactionId | String | 可选 | 事务ID |

### 4.2 消息拉取Header

**PullMessageRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| consumerGroup | String | 是 | 消费者组名 |
| topic | String | 是 | Topic名称 |
| queueId | Integer | 是 | 队列ID |
| queueOffset | Long | 是 | 拉取起始偏移量 |
| maxMsgNums | Integer | 是 | 最大拉取消息数 |
| sysFlag | Integer | 是 | 系统标志 |
| commitOffset | Long | 是 | 提交偏移量 |
| suspendTimeoutMillis | Long | 是 | 挂起超时时间（长轮询） |
| subscription | String | 可选 | 订阅表达式 |
| subVersion | Long | 是 | 订阅版本 |
| expressionType | String | 可选 | 表达式类型（TAG/SQL92） |

**PullMessageResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| suggestWhichBrokerId | Long | 是 | 建议拉取的Broker ID |
| nextBeginOffset | Long | 是 | 下次拉取起始偏移量 |
| minOffset | Long | 是 | 最小偏移量 |
| maxOffset | Long | 是 | 最大偏移量 |

### 4.3 消费进度Header

**QueryConsumerOffsetRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| consumerGroup | String | 是 | 消费者组名 |
| topic | String | 是 | Topic名称 |
| queueId | Integer | 是 | 队列ID |

**QueryConsumerOffsetResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| offset | Long | 是 | 消费偏移量 |

**UpdateConsumerOffsetRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| consumerGroup | String | 是 | 消费者组名 |
| topic | String | 是 | Topic名称 |
| queueId | Integer | 是 | 队列ID |
| commitOffset | Long | 是 | 提交的偏移量 |

### 4.4 客户端管理Header

**UnregisterClientRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| clientID | String | 是 | 客户端ID |
| producerGroup | String | 可选 | 生产者组名 |
| consumerGroup | String | 可选 | 消费者组名 |

### 4.5 消息查询Header

**QueryMessageRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| topic | String | 是 | Topic名称 |
| key | String | 是 | 消息Key |
| maxNum | Integer | 是 | 最大返回数 |
| beginTimestamp | Long | 是 | 起始时间戳 |
| endTimestamp | Long | 是 | 结束时间戳 |

**ViewMessageRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| offset | Long | 是 | 消息偏移量 |

### 4.6 偏移量查询Header

**GetMaxOffsetRequestHeader / GetMaxOffsetResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| topic | String | 是 | Topic名称（请求） |
| queueId | Integer | 是 | 队列ID（请求） |
| offset | Long | 是 | 最大偏移量（响应） |

**GetMinOffsetRequestHeader / GetMinOffsetResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| topic | String | 是 | Topic名称（请求） |
| queueId | Integer | 是 | 队列ID（请求） |
| offset | Long | 是 | 最小偏移量（响应） |

**SearchOffsetRequestHeader / SearchOffsetResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| topic | String | 是 | Topic名称（请求） |
| queueId | Integer | 是 | 队列ID（请求） |
| timestamp | Long | 是 | 时间戳（请求） |
| offset | Long | 是 | 搜索到的偏移量（响应） |

**GetEarliestMsgStoretimeRequestHeader / GetEarliestMsgStoretimeResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| topic | String | 是 | Topic名称（请求） |
| queueId | Integer | 是 | 队列ID（请求） |
| timestamp | Long | 是 | 最早存储时间（响应） |

### 4.7 Topic管理Header

**CreateTopicRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| topic | String | 是 | Topic名称 |
| defaultTopic | String | 是 | 默认Topic |
| readQueueNums | Integer | 是 | 读队列数 |
| writeQueueNums | Integer | 是 | 写队列数 |
| perm | Integer | 是 | 权限 |
| topicFilterType | String | 可选 | 过滤类型 |
| topicSysFlag | Integer | 可选 | 系统标志 |
| order | Boolean | 可选 | 是否有序 |

### 4.8 事务Header

**EndTransactionRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| producerGroup | String | 是 | 生产者组名 |
| tranStateTableOffset | Long | 是 | 事务状态表偏移量 |
| commitLogOffset | Long | 是 | CommitLog偏移量 |
| commitOrRollback | Integer | 是 | 提交或回滚 |
| fromTransactionCheck | Boolean | 可选 | 是否来自事务检查 |
| msgId | String | 是 | 消息ID |
| transactionId | String | 可选 | 事务ID |

**CheckTransactionStateRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| transactionId | String | 可选 | 事务ID |
| commitLogOffset | Long | 是 | CommitLog偏移量 |
| msgId | String | 是 | 消息ID |
| tranStateTableOffset | Long | 是 | 事务状态表偏移量 |

### 4.9 消费者管理Header

**GetConsumerListByGroupRequestHeader / GetConsumerListByGroupResponseHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| consumerGroup | String | 是 | 消费者组名（请求） |
| clientId | String | 是 | 客户端ID（响应） |

**GetConsumerConnectionListRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| consumerGroup | String | 是 | 消费者组名 |

**GetProducerConnectionListRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| producerGroup | String | 是 | 生产者组名 |

**ConsumerSendMsgBackRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| offset | Long | 是 | 消息偏移量 |
| group | String | 是 | 消费者组名 |
| delayLevel | Integer | 是 | 延迟级别 |
| originMsgId | String | 可选 | 原始消息ID |
| originTopic | String | 可选 | 原始Topic |
| unitMode | Boolean | 可选 | 单元模式 |
| maxReconsumeTimes | Integer | 可选 | 最大重试次数 |

### 4.10 回复消息Header

**ReplyMessageRequestHeader**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| producerGroup | String | 是 | 生产者组名 |
| topic | String | 是 | Topic名称 |
| defaultTopic | String | 是 | 默认Topic |
| defaultTopicQueueNums | Integer | 是 | 默认Topic队列数 |
| queueId | Integer | 是 | 队列ID |
| sysFlag | Integer | 是 | 系统标志 |
| bornTimestamp | Long | 是 | 消息产生时间戳 |
| flag | Integer | 是 | 消息标志 |
| properties | String | 可选 | 消息属性 |
| reconsumeTimes | Integer | 可选 | 重试次数 |
| unitMode | boolean | 可选 | 单元模式 |
| bornHost | String | 可选 | 来源主机 |

---

## 5. 分阶段适配计划

### 阶段一：核心消息链路（最小可用）

**目标**：实现普通消息的发送和拉取，客户端能正常收发消息

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P0 | SEND_MESSAGE | 10 | 消息发送V1 | 代理转发 |
| P0 | SEND_MESSAGE_V2 | 310 | 消息发送V2 | 代理转发 |
| P0 | SEND_BATCH_MESSAGE | 320 | 批量发送 | 代理转发 |
| P0 | PULL_MESSAGE | 11 | 消息拉取 | 代理转发 |
| P0 | QUERY_CONSUMER_OFFSET | 14 | 查询消费进度 | 代理转发 |
| P0 | UPDATE_CONSUMER_OFFSET | 15 | 更新消费进度 | 代理转发 |
| P0 | GET_ROUTEINFO_BY_TOPIC | 105 | 获取路由信息 | 虚拟路由代理 |
| P0 | HEART_BEAT | 34 | 心跳 | 代理转发 |
| P0 | UNREGISTER_CLIENT | 35 | 客户端注销 | 代理转发 |
| P0 | REGISTER_BROKER | 103 | Broker注册 | Proxy注册到NameServer |

**涉及Header类**：
- SendMessageRequestHeader / SendMessageRequestHeaderV2 / SendMessageResponseHeader
- PullMessageRequestHeader / PullMessageResponseHeader
- QueryConsumerOffsetRequestHeader / QueryConsumerOffsetResponseHeader
- UpdateConsumerOffsetRequestHeader
- UnregisterClientRequestHeader
- RegisterBrokerRequestHeader

**验证标准**：
- 原生RocketMQ客户端能通过Proxy正常发送和消费普通消息
- 消费进度正常维护
- 客户端启停正常

---

### 阶段二：消费管理增强

**目标**：支持消费者组管理、消息重试、队列分配

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P1 | GET_CONSUMER_LIST_BY_GROUP | 38 | 获取消费者列表 | 代理转发 |
| P1 | LOCK_BATCH_MQ | 41 | 批量锁定队列 | 代理转发 |
| P1 | UNLOCK_BATCH_MQ | 42 | 批量解锁队列 | 代理转发 |
| P1 | CONSUMER_SEND_MSG_BACK | 36 | 消息消费失败发回 | 代理转发 |
| P1 | QUERY_BROKER_OFFSET | 13 | 查询Broker偏移量 | 代理转发 |
| P1 | GET_MAX_OFFSET | 30 | 获取最大偏移量 | 代理转发 |
| P1 | GET_MIN_OFFSET | 31 | 获取最小偏移量 | 代理转发 |
| P1 | SEARCH_OFFSET_BY_TIMESTAMP | 29 | 按时间戳搜索偏移量 | 代理转发 |
| P1 | GET_EARLIEST_MSG_STORETIME | 32 | 获取最早消息存储时间 | 代理转发 |
| P1 | GET_ALL_CONSUMER_OFFSET | 43 | 获取所有消费者偏移量 | 代理转发 |
| P1 | GET_ALL_DELAY_OFFSET | 45 | 获取所有延迟偏移量 | 代理转发 |
| P1 | NOTIFY_CONSUMER_IDS_CHANGED | 40 | 通知消费者ID变更 | 代理转发 |

**涉及Header类**：
- GetConsumerListByGroupRequestHeader / GetConsumerListByGroupResponseHeader
- ConsumerSendMsgBackRequestHeader
- GetMaxOffsetRequestHeader / GetMaxOffsetResponseHeader
- GetMinOffsetRequestHeader / GetMinOffsetResponseHeader
- SearchOffsetRequestHeader / SearchOffsetResponseHeader
- GetEarliestMsgStoretimeRequestHeader / GetEarliestMsgStoretimeResponseHeader
- NotifyConsumerIdsChangedRequestHeader

**验证标准**：
- 消费者组Rebalance正常
- 消息重试机制正常
- 偏移量查询正常

---

### 阶段三：消息查询与Topic管理

**目标**：支持消息查询、Topic管理、运维操作

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P2 | QUERY_MESSAGE | 12 | 查询消息 | 代理转发 |
| P2 | VIEW_MESSAGE_BY_ID | 33 | 按ID查看消息 | 代理转发 |
| P2 | UPDATE_AND_CREATE_TOPIC | 17 | 创建/更新Topic | 代理转发 |
| P2 | GET_ALL_TOPIC_CONFIG | 21 | 获取所有Topic配置 | 代理转发 |
| P2 | GET_TOPIC_CONFIG_LIST | 22 | 获取Topic配置列表 | 代理转发 |
| P2 | GET_TOPIC_NAME_LIST | 23 | 获取Topic名称列表 | 代理转发 |
| P2 | DELETE_TOPIC_IN_BROKER | 215 | 删除Broker上的Topic | 代理转发 |
| P2 | GET_ALL_TOPIC_LIST_FROM_NAMESERVER | 206 | 获取所有Topic列表 | 代理转发 |
| P2 | GET_BROKER_CLUSTER_INFO | 106 | 获取Broker集群信息 | 代理转发 |
| P2 | UNREGISTER_BROKER | 104 | Broker注销 | 代理转发 |
| P2 | GET_SYSTEM_TOPIC_LIST_FROM_BROKER | 305 | 获取系统Topic列表 | 代理转发 |
| P2 | DELETE_TOPIC_IN_NAMESRV | 216 | 删除NameServer上的Topic | 代理转发 |

**涉及Header类**：
- QueryMessageRequestHeader / QueryMessageResponseHeader
- ViewMessageRequestHeader / ViewMessageResponseHeader
- CreateTopicRequestHeader
- DeleteTopicRequestHeader
- UnregisterBrokerRequestHeader

**验证标准**：
- 消息查询功能正常
- Topic创建/删除正常
- 管理工具能正常操作

---

### 阶段四：消费者运维与管理

**目标**：支持消费者运维、订阅组管理、消费统计

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P2 | UPDATE_AND_CREATE_SUBSCRIPTIONGROUP | 200 | 创建/更新订阅组 | 代理转发 |
| P2 | GET_ALL_SUBSCRIPTIONGROUP_CONFIG | 201 | 获取所有订阅组配置 | 代理转发 |
| P2 | DELETE_SUBSCRIPTIONGROUP | 207 | 删除订阅组 | 代理转发 |
| P2 | GET_CONSUMER_CONNECTION_LIST | 203 | 获取消费者连接列表 | 代理转发 |
| P2 | GET_PRODUCER_CONNECTION_LIST | 204 | 获取生产者连接列表 | 代理转发 |
| P2 | GET_CONSUME_STATS | 208 | 获取消费统计 | 代理转发 |
| P2 | GET_TOPIC_STATS_INFO | 202 | 获取Topic统计信息 | 代理转发 |
| P2 | QUERY_TOPIC_CONSUME_BY_WHO | 300 | 查询Topic被谁消费 | 代理转发 |
| P2 | QUERY_CONSUME_TIME_SPAN | 303 | 查询消费时间跨度 | 代理转发 |
| P2 | GET_CONSUMER_RUNNING_INFO | 307 | 获取消费者运行信息 | 代理转发 |
| P2 | INVOKE_BROKER_TO_RESET_OFFSET | 222 | 通知Broker重置偏移量 | 代理转发 |
| P2 | INVOKE_BROKER_TO_GET_CONSUMER_STATUS | 223 | 获取消费者状态 | 代理转发 |
| P2 | CLONE_GROUP_OFFSET | 314 | 克隆组偏移量 | 代理转发 |
| P2 | RESET_CONSUMER_OFFSET_IN_BROKER | 212 | 重置消费者偏移量 | 代理转发 |
| P2 | GET_BROKER_CONSUME_STATS | 317 | 获取Broker消费统计 | 代理转发 |

**涉及Header类**：
- DeleteSubscriptionGroupRequestHeader
- GetConsumerConnectionListRequestHeader
- GetProducerConnectionListRequestHeader
- GetConsumeStatsRequestHeader
- GetTopicStatsInfoRequestHeader
- QueryTopicConsumeByWhoRequestHeader
- QueryConsumeTimeSpanRequestHeader
- GetConsumerRunningInfoRequestHeader
- ResetOffsetRequestHeader
- GetConsumerStatusRequestHeader
- CloneGroupOffsetRequestHeader
- GetConsumeStatsInBrokerHeader

**验证标准**：
- 消费者运维操作正常
- 订阅组管理正常
- 消费统计查询正常

---

### 阶段五：事务消息与RPC回复

**目标**：支持事务消息、RPC回复消息

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P3 | END_TRANSACTION | 37 | 结束事务 | 代理转发 |
| P3 | CHECK_TRANSACTION_STATE | 39 | 事务状态回查 | 代理转发 |
| P3 | SEND_REPLY_MESSAGE | 324 | 发送回复消息 | 代理转发 |
| P3 | SEND_REPLY_MESSAGE_V2 | 325 | 发送回复消息V2 | 代理转发 |
| P3 | PUSH_REPLY_MESSAGE_TO_CLIENT | 326 | 推送回复消息到客户端 | 代理转发 |
| P3 | RESUME_CHECK_HALF_MESSAGE | 323 | 恢复检查半消息 | 代理转发 |

**涉及Header类**：
- EndTransactionRequestHeader / EndTransactionResponseHeader
- CheckTransactionStateRequestHeader / CheckTransactionStateResponseHeader
- ReplyMessageRequestHeader
- ResumeCheckHalfMessageRequestHeader

**验证标准**：
- 事务消息发送/提交/回滚正常
- RPC回复消息正常
- 半消息恢复检查正常

---

### 阶段六：ACL权限与高级运维

**目标**：支持ACL权限管理、高级运维操作

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P3 | UPDATE_AND_CREATE_ACL_CONFIG | 50 | 创建/更新ACL配置 | 代理转发 |
| P3 | DELETE_ACL_CONFIG | 51 | 删除ACL配置 | 代理转发 |
| P3 | GET_BROKER_CLUSTER_ACL_INFO | 52 | 获取Broker集群ACL信息 | 代理转发 |
| P3 | UPDATE_GLOBAL_WHITE_ADDRS_CONFIG | 53 | 更新全局白名单 | 代理转发 |
| P3 | UPDATE_BROKER_CONFIG | 25 | 更新Broker配置 | 代理转发 |
| P3 | GET_BROKER_CONFIG | 26 | 获取Broker配置 | 代理转发 |
| P3 | GET_BROKER_RUNTIME_INFO | 28 | 获取Broker运行信息 | 代理转发 |
| P3 | TRIGGER_DELETE_FILES | 27 | 触发删除文件 | 代理转发 |
| P3 | DELETE_EXPIRED_COMMITLOG | 329 | 删除过期CommitLog | 代理转发 |
| P3 | CLEAN_EXPIRED_CONSUMEQUEUE | 306 | 清理过期消费队列 | 代理转发 |
| P3 | CLEAN_UNUSED_TOPIC | 316 | 清理未使用Topic | 代理转发 |
| P3 | QUERY_CORRECTION_OFFSET | 308 | 查询修正偏移量 | 代理转发 |
| P3 | CONSUME_MESSAGE_DIRECTLY | 309 | 直接消费消息 | 代理转发 |
| P3 | VIEW_BROKER_STATS_DATA | 315 | 查看Broker统计数据 | 代理转发 |
| P3 | QUERY_CONSUME_QUEUE | 321 | 查询消费队列 | 代理转发 |
| P3 | GET_ALL_PRODUCER_INFO | 328 | 获取所有生产者信息 | 代理转发 |
| P3 | CHECK_CLIENT_CONFIG | 46 | 检查客户端配置 | 代理转发 |
| P3 | REGISTER_FILTER_SERVER | 301 | 注册过滤服务器 | 代理转发 |
| P3 | REGISTER_MESSAGE_FILTER_CLASS | 302 | 注册消息过滤类 | 代理转发 |
| P3 | WIPE_WRITE_PERM_OF_BROKER | 205 | 清除Broker写权限 | 代理转发 |
| P3 | ADD_WRITE_PERM_OF_BROKER | 327 | 增加Broker写权限 | 代理转发 |
| P3 | SUSPEND_CONSUMER | 209 | 暂停消费者 | 代理转发 |
| P3 | RESUME_CONSUMER | 210 | 恢复消费者 | 代理转发 |
| P3 | ADJUST_CONSUMER_THREAD_POOL | 213 | 调整消费者线程池 | 代理转发 |
| P3 | WHO_CONSUME_THE_MESSAGE | 214 | 查询谁消费了消息 | 代理转发 |
| P3 | RESET_CONSUMER_CLIENT_OFFSET | 220 | 重置消费者客户端偏移量 | 代理转发 |
| P3 | GET_CONSUMER_STATUS_FROM_CLIENT | 221 | 从客户端获取消费状态 | 代理转发 |
| P3 | RESET_CONSUMER_OFFSET_IN_CONSUMER | 211 | 重置消费者偏移量(消费者侧) | 代理转发 |

**涉及Header类**：
- CreateAccessConfigRequestHeader
- DeleteAccessConfigRequestHeader
- UpdateGlobalWhiteAddrsConfigRequestHeader
- QueryCorrectionOffsetHeader
- ConsumeMessageDirectlyResultRequestHeader
- ViewBrokerStatsDataRequestHeader
- QueryConsumeQueueRequestHeader
- GetAllProducerInfoRequestHeader

**验证标准**：
- ACL权限管理正常
- Broker运维操作正常
- 高级诊断功能正常

---

### 阶段七：NameServer完整协议与KV管理

**目标**：完整支持NameServer协议

| 优先级 | 请求码 | 值 | 功能 | 适配方式 |
|--------|--------|-----|------|---------|
| P3 | PUT_KV_CONFIG | 100 | 存储KV配置 | 代理转发 |
| P3 | GET_KV_CONFIG | 101 | 获取KV配置 | 代理转发 |
| P3 | DELETE_KV_CONFIG | 102 | 删除KV配置 | 代理转发 |
| P3 | GET_KVLIST_BY_NAMESPACE | 219 | 按命名空间获取KV列表 | 代理转发 |
| P3 | GET_TOPICS_BY_CLUSTER | 224 | 按集群获取Topic列表 | 代理转发 |
| P3 | GET_SYSTEM_TOPIC_LIST_FROM_NS | 304 | 获取系统Topic列表 | 代理转发 |
| P3 | GET_UNIT_TOPIC_LIST | 311 | 获取单元Topic列表 | 代理转发 |
| P3 | GET_HAS_UNIT_SUB_TOPIC_LIST | 312 | 获取含单元子Topic列表 | 代理转发 |
| P3 | GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST | 313 | 获取含单元子非单元Topic列表 | 代理转发 |
| P3 | UPDATE_NAMESRV_CONFIG | 318 | 更新NameServer配置 | 代理转发 |
| P3 | GET_NAMESRV_CONFIG | 319 | 获取NameServer配置 | 代理转发 |
| P3 | QUERY_DATA_VERSION | 322 | 查询数据版本 | 代理转发 |

**涉及Header类**：
- GetTopicsByClusterRequestHeader

**验证标准**：
- NameServer协议完全兼容
- KV配置管理正常
- 集群管理正常

---

## 6. 适配统计

| 阶段 | 请求码数量 | 核心目标 |
|------|-----------|---------|
| 阶段一 | 10 | 核心消息链路（最小可用） |
| 阶段二 | 12 | 消费管理增强 |
| 阶段三 | 12 | 消息查询与Topic管理 |
| 阶段四 | 15 | 消费者运维与管理 |
| 阶段五 | 6 | 事务消息与RPC回复 |
| 阶段六 | 27 | ACL权限与高级运维 |
| 阶段七 | 12 | NameServer完整协议与KV管理 |
| **合计** | **94** | **完整协议兼容** |

> 注：部分请求码在不同处理器中重复出现（如GET_CONSUMER_RUNNING_INFO在Broker和Client都有），按实际使用场景归入对应阶段