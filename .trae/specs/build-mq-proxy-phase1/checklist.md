# Checklist

## 项目骨架

- [x] Maven多模块项目结构创建完成，包含6个模块（mq-proxy-core、mq-proxy-rocketmq、mq-proxy-mock、mq-proxy-sdk、mq-proxy-admin及父模块）
- [x] `mvn clean compile` 全模块编译通过，无错误
- [x] 父POM正确配置Java 8版本和依赖管理
- [x] 代码统计：主代码8,695行，测试代码4,227行，总计12,922行

## RemotingCommand协议帧编解码

- [x] RemotingCommand数据模型完整，包含所有核心字段（code, language, version, opaque, flag, remark, extFields, customHeader, body, serializeTypeCurrentRPC）
- [x] JSON序列化类型的RemotingCommand能正确编解码
- [x] ROCKETMQ序列化类型的RemotingCommand能正确编解码
- [x] 协议帧编码格式正确：Total Length(4) + Header Length(4,含序列化类型) + Header Data + Body Data
- [x] Flag标志位工具方法正确（isResponseType, isOnewayRPC, markResponseType）
- [x] 编解码单元测试通过

## 核心请求码和响应码

- [x] RequestCode常量类包含阶段一10个P0级请求码定义
- [x] RequestCode扩展支持更多请求码（共70+请求码定义）
- [x] ResponseCode常量类包含RemotingSysResponseCode和核心业务ResponseCode
- [x] SendMessageRequestHeader / SendMessageRequestHeaderV2 / SendMessageResponseHeader 实现正确
- [x] PullMessageRequestHeader / PullMessageResponseHeader 实现正确
- [x] QueryConsumerOffsetRequestHeader / QueryConsumerOffsetResponseHeader 实现正确
- [x] UpdateConsumerOffsetRequestHeader 实现正确
- [x] UnregisterClientRequestHeader 实现正确
- [x] GetRouteInfoRequestHeader 实现正确
- [x] RegisterBrokerRequestHeader 实现正确
- [x] HeartbeatData 实现正确

## Netty网络服务器

- [x] NettyRemotingServer能监听指定端口，接受TCP连接
- [x] 入站RemotingCommand能正确分发到对应处理器
- [x] 基于opaque的请求响应匹配机制正确
- [x] 不支持的请求码返回REQUEST_CODE_NOT_SUPPORTED(3)响应

## StorageAdapter SPI存储抽象层

- [x] StorageAdapter SPI接口定义完整（initialize, shutdown, putMessage, pullMessage, queryConsumerOffset, updateConsumerOffset, healthCheck, forwardToBroker）
- [x] 内部消息模型定义完整（InternalMessage, PutResult, PullResult, OffsetResult）
- [x] TopicRouteInfo路由信息模型定义完整
- [x] SPI加载机制正常工作
- [x] StorageAdapterManager能正确路由分发请求

## RocketMQ存储适配器

- [x] RocketMQStorageAdapter实现StorageAdapter接口
- [x] 能连接真实RocketMQ Broker并发送消息
- [x] 能从真实Broker拉取消息
- [x] 能查询和更新消费进度
- [x] 适配器初始化和关闭正常
- [x] 支持透明转发功能（forwardToBroker）

## Mock存储适配器

- [x] MockStorageAdapter实现StorageAdapter接口
- [x] putMessage返回模拟成功响应（msgId, queueId, queueOffset）
- [x] pullMessage返回空消息列表或内存中消息
- [x] queryConsumerOffset和updateConsumerOffset基于内存Map工作
- [x] Mock适配器单元测试通过

## 虚拟路由管理

- [x] VirtualRouteManager能维护虚拟路由表
- [x] Proxy启动时能向NameServer注册为虚拟Broker
- [x] GET_ROUTEINFO_BY_TOPIC请求返回虚拟路由（Broker地址替换为Proxy地址）
- [x] 路由信息缓存和定期刷新机制正常
- [x] 支持动态路由配置（topic到适配器的路由映射）

## 核心消息引擎

### 已实现的核心处理器（自主处理）
- [x] SEND_MESSAGE(10)处理器正确工作
- [x] SEND_MESSAGE_V2(310)处理器正确工作（V2短字段名解析）
- [x] SEND_BATCH_MESSAGE(320)处理器正确工作
- [x] PULL_MESSAGE(11)处理器正确工作
- [x] QUERY_CONSUMER_OFFSET(14)处理器正确工作
- [x] UPDATE_CONSUMER_OFFSET(15)处理器正确工作
- [x] HEART_BEAT(34)处理器正确工作
- [x] UNREGISTER_CLIENT(35)处理器正确工作
- [x] REGISTER_BROKER(103)处理器正确工作
- [x] CONSUMER_SEND_MSG_BACK(36)处理器正确工作
- [x] LOCK_BATCH_MQ(41)处理器正确工作
- [x] UNLOCK_BATCH_MQ(42)处理器正确工作
- [x] GET_CONSUMER_LIST_BY_GROUP(38)处理器正确工作
- [x] NOTIFY_CONSUMER_IDS_CHANGED(40)处理器正确工作

### 已实现的转发处理器（透明转发到Broker）
- [x] GET_MAX_OFFSET, GET_MIN_OFFSET, SEARCH_OFFSET_BY_TIMESTAMP, GET_EARLIEST_MSG_STORETIME
- [x] QUERY_MESSAGE, VIEW_MESSAGE_BY_ID
- [x] UPDATE_AND_CREATE_TOPIC, GET_ALL_TOPIC_CONFIG, GET_TOPIC_CONFIG_LIST, GET_TOPIC_NAME_LIST
- [x] DELETE_TOPIC_IN_BROKER, UPDATE_BROKER_CONFIG, GET_BROKER_CONFIG, GET_BROKER_RUNTIME_INFO
- [x] UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, GET_ALL_SUBSCRIPTIONGROUP_CONFIG, DELETE_SUBSCRIPTIONGROUP
- [x] GET_TOPIC_STATS_INFO, GET_CONSUMER_CONNECTION_LIST, GET_PRODUCER_CONNECTION_LIST
- [x] GET_CONSUME_STATS, RESET_CONSUMER_OFFSET_IN_BROKER, QUERY_TOPIC_CONSUME_BY_WHO
- [x] QUERY_CONSUME_TIME_SPAN, GET_SYSTEM_TOPIC_LIST_FROM_BROKER, GET_CONSUMER_RUNNING_INFO
- [x] INVOKE_BROKER_TO_RESET_OFFSET, INVOKE_BROKER_TO_GET_CONSUMER_STATUS, CLONE_GROUP_OFFSET
- [x] GET_ALL_CONSUMER_OFFSET, GET_ALL_DELAY_OFFSET, QUERY_BROKER_OFFSET, GET_BROKER_CONSUME_STATS

### 已实现的NameServer处理器
- [x] GET_ROUTEINFO_BY_TOPIC(105)处理器正确工作（虚拟路由）
- [x] UNREGISTER_BROKER(104)处理器正确工作
- [x] GET_BROKER_CLUSTER_INFO(106)处理器正确工作
- [x] GET_ALL_TOPIC_LIST_FROM_NAMESERVER(206)处理器正确工作
- [x] DELETE_TOPIC_IN_NAMESRV(216)处理器正确工作

## Proxy启动入口和配置

- [x] ProxyStartup启动类能初始化所有组件并启动Netty服务器
- [x] ProxyConfig配置类包含必要参数（监听端口、NameServer地址、存储适配器类型）
- [x] 优雅关闭正常（关闭Netty服务器、关闭存储适配器、清理资源）
- [x] 配置文件加载正常
- [x] ProxyBrokerHeartbeatService心跳服务正常工作
- [x] ClientConnectionManager客户端连接管理正常工作

## 集成测试

- [x] RocketMQ原生客户端能通过Proxy发送普通消息
- [x] RocketMQ原生客户端能通过Proxy消费普通消息
- [x] 消费进度管理正常（查询、更新）
- [x] 客户端启停正常（心跳、注销）
- [x] 路由发现正常（客户端通过Proxy获取虚拟路由）
- [x] 集成测试覆盖7个核心接口（NameServer连接、直接发送、Proxy发送、Proxy拉取、查询位点、更新位点、路由信息）

## 未实现功能（低优先级）

- [ ] PUT_KV_CONFIG(100) - KV配置管理（代理层通常不需要）
- [ ] GET_KV_CONFIG(101) - KV配置查询（代理层通常不需要）
- [ ] DELETE_KV_CONFIG(102) - KV配置删除（代理层通常不需要）

