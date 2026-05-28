# MQ Proxy Standalone

MQ Proxy 是一个 RocketMQ 代理中间件，位于客户端和 Broker 之间，支持消息收发、消费、路由转发等功能。

每个 Proxy 实例代理一套 RocketMQ Broker 集群（1:1 关系），不配置 `brokerAddr` 时自动使用 Mock 模式（内存存储，无需 Broker）。

## 快速开始

### 1. 打包

```bash
mvn package -pl mq-proxy-standalone -am -DskipTests
```

生成文件：
- `target/mq-proxy/` — 可直接运行的目录
- `target/mq-proxy-1.0.0-SNAPSHOT.tar.gz` — 分发包

### 2. 解压

```bash
tar -xzf target/mq-proxy-1.0.0-SNAPSHOT.tar.gz
cd mq-proxy-1.0.0-SNAPSHOT
```

### 3. 修改配置

编辑 `conf/proxy.properties`，至少修改以下配置：

```properties
proxy.listenPort=10913
proxy.namesrvAddr=127.0.0.1:9876
proxy.brokerAddr=127.0.0.1:10911
```

> 不配置 `proxy.brokerAddr` 时，Proxy 自动以 Mock 模式运行，消息存储在内存中，无需启动 NameServer 和 Broker。

### 4. 启动

```bash
bash bin/proxy.sh
```

### 5. 客户端连接

将客户端的 `namesrvAddr` 指向 Proxy 即可：

```java
// 生产者
DefaultMQProducer producer = new DefaultMQProducer("producer_group");
producer.setNamesrvAddr("127.0.0.1:10913");
producer.start();

// 消费者
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
consumer.setNamesrvAddr("127.0.0.1:10913");
consumer.subscribe("TopicTest", "*");
consumer.start();
```

---

## 目录结构

```
mq-proxy/
├── bin/
│   └── proxy.sh              # 启动脚本
├── conf/
│   ├── proxy.properties      # 主配置文件
│   └── logback.xml           # 日志配置
├── lib/                      # 依赖 jar 包
└── logs/                     # 运行日志
    ├── proxy.log             # 主日志
    └── gc.log                # GC 日志
```

---

## 配置详解

### 网络连接

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `proxy.listenPort` | 10911 | Proxy 对外监听端口，客户端连接此端口 |
| `proxy.host` | 127.0.0.1 | Proxy 自身 IP，用于向 NameServer 注册时告知客户端回连地址 |
| `proxy.namesrvAddr` | 127.0.0.1:9876 | NameServer 地址，多个用分号分隔 |
| `proxy.brokerAddr` | 空 | Broker 地址，Proxy 转发消息的目标；不配置时使用 Mock 模式 |
| `proxy.connectTimeoutMillis` | 3000 | Proxy 连接 Broker 的超时时间（毫秒） |

### 多 NameServer 配置

NameServer 地址支持分号分隔，与 RocketMQ 原生格式一致：

```properties
proxy.namesrvAddr=10.0.0.1:9876;10.0.0.2:9876
```

### 路由注册

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `proxy.registerProxyToNameServer` | false | 是否将 Proxy 注册到 NameServer |
| `proxy.proxyBrokerName` | ProxyBroker | Proxy 注册时使用的 Broker 名称 |
| `proxy.proxyClusterName` | ProxyCluster | Proxy 注册时使用的集群名称 |
| `proxy.routeCacheExpireMillis` | 30000 | 路由缓存过期时间（毫秒） |

**`proxy.proxyBrokerName` 说明**：

此值必须与真实 Broker 的 `brokerName` 一致。当 `registerProxyToNameServer=true` 时，
Proxy 会把自己注册为该 Broker 的一个节点，客户端从 NameServer 获取路由时会拿到 Proxy 地址，
从而自动通过 Proxy 通信，无需修改客户端代码。

### Netty 线程

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `proxy.bossThreadNums` | 1 | Boss 线程数，负责接受新连接 |
| `proxy.workerThreadNums` | CPU 核心数 | Worker 线程数，处理 I/O 事件 |

### TLS 配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `proxy.tlsEnabled` | false | 是否启用 TLS，启用后客户端必须通过 TLS 连接 Proxy |
| `proxy.tlsCertPath` | 空 | 服务端证书文件路径（PEM 格式，如 `/etc/mq-proxy/server.crt`），启用 TLS 时必填 |
| `proxy.tlsKeyPath` | 空 | 服务端私钥文件路径（PEM 格式，如 `/etc/mq-proxy/server.key`），启用 TLS 时必填 |
| `proxy.tlsTrustCertPath` | 空 | 受信 CA 证书文件路径（PEM 格式），双向认证时必填，用于验证客户端证书 |
| `proxy.tlsClientAuth` | false | 是否要求客户端提供证书；`true` 表示启用双向认证（mTLS），需配合 `tlsTrustCertPath` 使用 |

> **说明**：Proxy 使用 PEM 格式证书，与 RocketMQ Broker 的 TLS 配置方式一致。如果私钥文件使用了密码加密，可通过 JVM 参数 `-Dproxy.tlsKeyPassword=xxx` 传入解密密码，大多数情况下私钥未加密，无需配置。

---

## TLS 证书生成与配置

Proxy 使用 PEM 格式证书，与 RocketMQ Broker 的 TLS 配置方式保持一致。

### 方式一：自签名证书（测试用）

#### 1. 生成服务端私钥和证书

```bash
openssl req -x509 -newkey rsa:2048 -keyout server.key \
  -out server.crt -days 3650 -nodes \
  -subj "/CN=MQ Proxy Server/OU=MQ/O=MyOrg/L=Beijing/ST=Beijing/C=CN"
```

#### 2. Proxy 配置（单向认证）

```properties
proxy.tlsEnabled=true
proxy.tlsCertPath=/path/to/server.crt
proxy.tlsKeyPath=/path/to/server.key
proxy.tlsClientAuth=false
```

#### 3. 客户端配置（单向认证）

```java
System.setProperty("rocketmq.tls.enable", "true");
System.setProperty("rocketmq.tls.trustcert", "/path/to/server.crt");
```

### 方式二：双向认证（mTLS，生产推荐）

在单向认证基础上，额外生成客户端证书：

#### 1. 生成客户端私钥和证书

```bash
openssl req -x509 -newkey rsa:2048 -keyout client.key \
  -out client.crt -days 3650 -nodes \
  -subj "/CN=MQ Proxy Client/OU=MQ/O=MyOrg/L=Beijing/ST=Beijing/C=CN"
```

#### 2. 生成服务端信任的 CA 证书链

将客户端证书作为受信证书，供服务端验证客户端：

```bash
cat client.crt > trusted-clients.crt
```

#### 3. Proxy 配置（双向认证）

```properties
proxy.tlsEnabled=true
proxy.tlsCertPath=/path/to/server.crt
proxy.tlsKeyPath=/path/to/server.key
proxy.tlsTrustCertPath=/path/to/trusted-clients.crt
proxy.tlsClientAuth=true
```

#### 4. 客户端配置（双向认证）

```java
System.setProperty("rocketmq.tls.enable", "true");
System.setProperty("rocketmq.tls.trustcert", "/path/to/server.crt");
System.setProperty("rocketmq.tls.cert", "/path/to/client.crt");
System.setProperty("rocketmq.tls.key", "/path/to/client.key");
```

### 方式三：使用 CA 签名证书（生产环境）

#### 1. 生成服务端私钥和 CSR

```bash
openssl req -newkey rsa:2048 -keyout server.key \
  -out server.csr -nodes \
  -subj "/CN=mq-proxy.example.com/OU=MQ/O=MyOrg/L=Beijing/ST=Beijing/C=CN"
```

#### 2. 用 CA 签名

```bash
openssl x509 -req \
  -in server.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out server.crt \
  -days 3650
```

#### 3. Proxy 配置

```properties
proxy.tlsEnabled=true
proxy.tlsCertPath=/path/to/server.crt
proxy.tlsKeyPath=/path/to/server.key
proxy.tlsTrustCertPath=/path/to/ca.crt
proxy.tlsClientAuth=true
```

> **提示**：以上示例中私钥均未加密（`-nodes` 参数）。如果私钥文件已加密，可通过 JVM 参数 `-Dproxy.tlsKeyPassword=xxx` 传入解密密码。

---

## 典型部署场景

### 场景一：开发测试

```
Client --> Proxy(10913) --> Broker(10911) --> NameServer(9876)
```

```properties
proxy.listenPort=10913
proxy.namesrvAddr=127.0.0.1:9876
proxy.brokerAddr=127.0.0.1:10911
```

### 场景二：生产透明代理

```
Client --> NameServer(9876) --> Proxy(10911) --> Broker(10911)
                ↑ Proxy 注册自己为 Broker 节点
```

```properties
proxy.listenPort=10911
proxy.host=10.0.0.5
proxy.namesrvAddr=10.0.0.1:9876;10.0.0.2:9876
proxy.brokerAddr=10.0.0.3:10911
proxy.registerProxyToNameServer=true
proxy.proxyBrokerName=broker-a
```

客户端无需修改 `namesrvAddr`，Proxy 透明代理所有请求。

### 场景三：TLS 加密 + 双向认证

```
Client --[TLS]--> Proxy --[TCP]--> Broker
```

```properties
proxy.tlsEnabled=true
proxy.tlsCertPath=/etc/mq-proxy/server.crt
proxy.tlsKeyPath=/etc/mq-proxy/server.key
proxy.tlsTrustCertPath=/etc/mq-proxy/ca.crt
proxy.tlsClientAuth=true
```

### 场景四：Mock 模式（无 Broker）

```
Client --> Proxy(10913) --> 内存存储
```

不配置 `proxy.brokerAddr` 即可，Proxy 自动使用 Mock 模式。

消息存储在内存中，适合功能验证和压测，无需启动 NameServer 和 Broker。

---

## 环境变量

启动脚本支持通过环境变量覆盖配置：

| 环境变量 | 说明 |
|----------|------|
| `JAVA_OPT` | 额外 JVM 参数 |
| `PROXY_HOME` | Proxy 安装目录（自动检测） |

示例：

```bash
JAVA_OPT="-Xmx2g" bash bin/proxy.sh
```

---

## 停止服务

```bash
kill <pid>
# 或
Ctrl+C
```

Proxy 注册了 ShutdownHook，会优雅关闭连接和释放资源。
