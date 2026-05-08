# MQ代理系统 - 阶段一：核心消息链路 Spec

## Why
当前系统使用RocketMQ作为消息中间件，后续可能需要替换底层存储实现或自研MQ系统。需要构建一个MQ代理系统，实现RocketMQ 4.9协议兼容，使现有RocketMQ客户端无需修改代码即可接入代理层，为后续后端存储替换提供透明代理能力。

## What Changes
- 创建Maven多模块项目骨架（mq-proxy-core、mq-proxy-rocketmq、mq-proxy-mock等）
- 实现RocketMQ 4.9 RemotingCommand协议帧的编解码（支持JSON和ROCKETMQ两种序列化）
- 实现基于Netty的网络服务器，接收RocketMQ原生客户端连接
- 实现阶段一核心请求码的协议处理器（10个P0级请求码）
- 实现StorageAdapter SPI存储抽象层接口
- 实现RocketMQ存储适配器（转发到真实RocketMQ Broker）
- 实现Mock存储适配器（用于开发测试）
- 实现虚拟路由管理（Proxy作为虚拟Broker注册到NameServer，返回虚拟路由给客户端）
- 实现核心消息引擎（消息路由、编解码、流控基础框架）

## Impact
- Affected specs: 新建项目，无已有功能受影响
- Affected code: 全新代码库，参考设计文档 `docs/specs/2026-04-28-mq-proxy-design.md` 和 `docs/specs/2026-04-28-rocketmq-protocol-adaptation.md`

## ADDED Requirements

### Requirement: 项目骨架搭建
系统SHALL提供Maven多模块项目结构，包含以下模块：
- `mq-proxy-core`：核心引擎（协议处理、消息引擎、存储抽象层）
- `mq-proxy-rocketmq`：RocketMQ存储适配器
- `mq-proxy-mock`：Mock存储适配器
- `mq-proxy-sdk`：proxy-SDK（阶段一仅预留模块）
- `mq-proxy-admin`：管理控制台（阶段一仅预留模块）

#### Scenario: 项目构建
- **WHEN** 执行 `mvn clean compile`
- **THEN** 所有模块编译成功，无错误

### Requirement: RemotingCommand协议帧编解码
系统SHALL支持RocketMQ 4.9 RemotingCommand协议帧的完整编解码。

#### Scenario: JSON序列化协议帧解码
- **WHEN** 接收到JSON序列化类型的RemotingCommand帧
- **THEN** 正确解析Total Length、Header Length、Header Data、Body Data各字段

#### Scenario: ROCKETMQ序列化协议帧解码
- **WHEN** 接收到ROCKETMQ序列化类型的RemotingCommand帧
- **THEN** 正确解析Total Length、Header Length、Header Data、Body Data各字段

#### Scenario: 协议帧编码
- **WHEN** 需要发送RemotingCommand响应
- **THEN** 正确编码为协议帧格式，包含正确的序列化类型标记

### Requirement: Netty网络服务器
系统SHALL提供基于Netty的网络服务器，监听指定端口，接收RocketMQ客户端的TCP连接。

#### Scenario: 客户端连接
- **WHEN** RocketMQ客户端连接到Proxy的监听端口
- **THEN** 成功建立TCP连接，客户端可发送RemotingCommand请求

#### Scenario: 请求响应匹配
- **WHEN** 客户端发送请求并等待响应
- **THEN** 通过opaque字段正确匹配请求和响应

### Requirement: 核心请求码协议处理
系统SHALL支持阶段一的10个P0级请求码处理：

| 请求码 | 值 | 功能 | 适配方式 |
|--------|-----|------|---------|
| SEND_MESSAGE | 10 | 消息发送V1 | 代理转发 |
| SEND_MESSAGE_V2 | 310 | 消息发送V2 | 代理转发 |
| SEND_BATCH_MESSAGE | 320 | 批量发送 | 代理转发 |
| PULL_MESSAGE | 11 | 消息拉取 | 代理转发 |
| QUERY_CONSUMER_OFFSET | 14 | 查询消费进度 | 代理转发 |
| UPDATE_CONSUMER_OFFSET | 15 | 更新消费进度 | 代理转发 |
| GET_ROUTEINFO_BY_TOPIC | 105 | 获取路由信息 | 虚拟路由代理 |
| HEART_BEAT | 34 | 心跳 | 代理转发 |
| UNREGISTER_CLIENT | 35 | 客户端注销 | 代理转发 |
| REGISTER_BROKER | 103 | Broker注册 | Proxy注册到NameServer |

#### Scenario: 消息发送V1
- **WHEN** 客户端发送SEND_MESSAGE(10)请求
- **THEN** Proxy解析SendMessageRequestHeader，通过存储适配器转发到后端Broker，返回SendMessageResponseHeader

#### Scenario: 消息发送V2
- **WHEN** 客户端发送SEND_MESSAGE_V2(310)请求
- **THEN** Proxy解析SendMessageRequestHeaderV2（短字段名），通过存储适配器转发，返回响应

#### Scenario: 批量消息发送
- **WHEN** 客户端发送SEND_BATCH_MESSAGE(320)请求
- **THEN** Proxy解析批量消息请求，通过存储适配器转发，返回响应

#### Scenario: 消息拉取
- **WHEN** 客户端发送PULL_MESSAGE(11)请求
- **THEN** Proxy解析PullMessageRequestHeader，通过存储适配器转发到后端Broker，返回PullMessageResponseHeader和消息体

#### Scenario: 查询消费进度
- **WHEN** 客户端发送QUERY_CONSUMER_OFFSET(14)请求
- **THEN** Proxy解析QueryConsumerOffsetRequestHeader，通过存储适配器查询，返回offset

#### Scenario: 更新消费进度
- **WHEN** 客户端发送UPDATE_CONSUMER_OFFSET(15)请求
- **THEN** Proxy解析UpdateConsumerOffsetRequestHeader，通过存储适配器更新

#### Scenario: 获取路由信息
- **WHEN** 客户端发送GET_ROUTEINFO_BY_TOPIC(105)请求
- **THEN** Proxy返回虚拟路由（Broker地址替换为Proxy地址），客户端以为连接的是真实Broker

#### Scenario: 心跳
- **WHEN** 客户端发送HEART_BEAT(34)请求
- **THEN** Proxy处理心跳，维护客户端连接状态

#### Scenario: 客户端注销
- **WHEN** 客户端发送UNREGISTER_CLIENT(35)请求
- **THEN** Proxy清理客户端相关资源

#### Scenario: Broker注册
- **WHEN** Proxy启动时
- **THEN** Proxy作为虚拟Broker注册到NameServer，使客户端能通过NameServer发现Proxy

### Requirement: StorageAdapter SPI存储抽象层
系统SHALL提供StorageAdapter SPI接口，屏蔽后端存储差异。

#### Scenario: 适配器加载
- **WHEN** Proxy启动时
- **THEN** 通过SPI机制加载配置的存储适配器实现

#### Scenario: 统一存储接口
- **WHEN** 消息引擎需要存储操作
- **THEN** 通过StorageAdapter统一接口调用，不感知后端存储类型

### Requirement: RocketMQ存储适配器
系统SHALL提供RocketMQ存储适配器实现，将请求转发到真实RocketMQ Broker。

#### Scenario: 消息发送转发
- **WHEN** 收到消息发送请求
- **THEN** 适配器将内部消息格式转换为RocketMQ协议，发送到真实Broker，返回结果

#### Scenario: 消息拉取转发
- **WHEN** 收到消息拉取请求
- **THEN** 适配器将请求转发到真实Broker，返回消息数据

#### Scenario: 消费进度操作转发
- **WHEN** 收到消费进度查询/更新请求
- **THEN** 适配器将请求转发到真实Broker

### Requirement: Mock存储适配器
系统SHALL提供Mock存储适配器，用于开发和测试环境。

#### Scenario: Mock消息发送
- **WHEN** 收到消息发送请求
- **THEN** Mock适配器模拟成功响应，返回msgId、queueId、queueOffset

#### Scenario: Mock消息拉取
- **WHEN** 收到消息拉取请求
- **THEN** Mock适配器返回空消息列表或预设消息

### Requirement: 虚拟路由管理
系统SHALL维护虚拟路由表，使客户端通过Proxy发现"虚拟Broker"。

#### Scenario: 路由信息转换
- **WHEN** 客户端请求Topic路由信息
- **THEN** Proxy从真实NameServer获取路由，将Broker地址替换为Proxy地址后返回给客户端

#### Scenario: Proxy注册为虚拟Broker
- **WHEN** Proxy启动时
- **THEN** Proxy向NameServer注册为虚拟Broker，包含Proxy的地址信息

### Requirement: 核心消息引擎
系统SHALL提供核心消息引擎，协调协议处理、消息路由和存储操作。

#### Scenario: 消息路由
- **WHEN** 收到消息发送请求
- **THEN** 根据Topic配置路由到对应的存储适配器

#### Scenario: 协议转换
- **WHEN** 协议处理器解析完客户端请求
- **THEN** 消息引擎将请求转换为内部消息模型，调用存储适配器，再将结果转换回协议响应

### Requirement: 不支持的请求码处理
系统SHALL对不支持的请求码返回REQUEST_CODE_NOT_SUPPORTED(3)响应。

#### Scenario: 未实现请求码
- **WHEN** 客户端发送阶段一未实现的请求码
- **THEN** 返回响应码为3（REQUEST_CODE_NOT_SUPPORTED）的RemotingCommand响应

### Requirement: 响应码定义
系统SHALL支持RocketMQ 4.9的系统响应码和核心业务响应码。

#### Scenario: 成功响应
- **WHEN** 请求处理成功
- **THEN** 返回响应码0（SUCCESS）

#### Scenario: 系统错误响应
- **WHEN** 处理过程中发生系统错误
- **THEN** 返回对应响应码（SYSTEM_ERROR=1, SYSTEM_BUSY=2等）
