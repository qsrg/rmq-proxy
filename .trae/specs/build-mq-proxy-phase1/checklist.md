# Checklist

## 项目骨架
- [x] Maven多模块项目结构创建完成，包含6个模块（mq-proxy-core、mq-proxy-rocketmq、mq-proxy-mock、mq-proxy-sdk、mq-proxy-admin及父模块）
- [x] `mvn clean compile` 全模块编译通过，无错误
- [x] 父POM正确配置Java 8版本和依赖管理

## RemotingCommand协议帧编解码
- [x] RemotingCommand数据模型完整，包含所有核心字段（code, language, version, opaque, flag, remark, extFields, customHeader, body, serializeTypeCurrentRPC）
- [x] JSON序列化类型的RemotingCommand能正确编解码
- [x] ROCKETMQ序列化类型的RemotingCommand能正确编解码
- [x] 协议帧编码格式正确：Total Length(4) + Header Length(4,含序列化类型) + Header Data + Body Data
- [x] Flag标志位工具方法正确（isResponseType, isOnewayRPC, markResponseType）
- [x] 编解码单元测试通过

## 核心请求码和响应码
- [x] RequestCode常量类包含阶段一10个P0级请求码定义
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
- [x] StorageAdapter SPI接口定义完整（initialize, shutdown, putMessage, pullMessage, queryConsumerOffset, updateConsumerOffset, healthCheck）
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

## 核心消息引擎
- [x] MessageEngine能协调协议处理、消息路由、存储操作
- [x] SEND_MESSAGE(10)处理器正确工作
- [x] SEND_MESSAGE_V2(310)处理器正确工作（V2短字段名解析）
- [x] SEND_BATCH_MESSAGE(320)处理器正确工作
- [x] PULL_MESSAGE(11)处理器正确工作
- [x] QUERY_CONSUMER_OFFSET(14)处理器正确工作
- [x] UPDATE_CONSUMER_OFFSET(15)处理器正确工作
- [x] HEART_BEAT(34)处理器正确工作
- [x] UNREGISTER_CLIENT(35)处理器正确工作
- [x] REGISTER_BROKER(103)处理器正确工作

## Proxy启动入口和配置
- [x] ProxyStartup启动类能初始化所有组件并启动Netty服务器
- [x] ProxyConfig配置类包含必要参数（监听端口、NameServer地址、存储适配器类型）
- [x] 优雅关闭正常（关闭Netty服务器、关闭存储适配器、清理资源）
- [x] 配置文件加载正常

## 集成测试
- [x] RocketMQ原生客户端能通过Proxy发送普通消息
- [x] RocketMQ原生客户端能通过Proxy消费普通消息
- [x] 消费进度管理正常（查询、更新）
- [x] 客户端启停正常（心跳、注销）
- [x] 路由发现正常（客户端通过Proxy获取虚拟路由）
