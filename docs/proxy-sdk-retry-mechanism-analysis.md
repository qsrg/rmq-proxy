# Proxy SDK 重试机制分析与测试报告

## 一、重试机制分析

### 1. 重试架构总览

Proxy SDK的重试机制采用**客户端主动重试**模式，整个重试流程在SDK端的`ProxyClientFacade.invokeSync`方法中实现。代理端和存储适配器不进行重试，只将失败状态向上传递。

### 2. 核心实现位置

**主要类：`ProxyClientFacade.java`**

重试循环的核心代码（第54-94行）：

```java
public RemotingCommand invokeSync(RemotingCommand request, long timeoutMillis)
        throws ProxyException {

    int maxRetryTimes = config.getRetryTimes();
    Exception lastException = null;

    for (int i = 0; i < maxRetryTimes; i++) {
        String proxyAddr = addressManager.selectProxyAddr();
        if (proxyAddr == null) {
            throw new ProxyConnectException("No available proxy address");
        }

        try {
            Channel channel = channelManager.getOrCreateChannel(proxyAddr);
            RemotingCommand response = remotingClient.invokeSync(channel, request, timeoutMillis);

            // 成功：清除故障标记，记录成功指标
            addressManager.clearFault(proxyAddr);
            metricsCollector.recordSuccess(proxyAddr, elapsed);

            return response;

        } catch (Exception e) {
            // 失败：标记故障，关闭通道，记录失败指标
            addressManager.markFault(proxyAddr);
            channelManager.closeChannel(proxyAddr);
            metricsCollector.recordFailure(proxyAddr, e);

            lastException = e;
            log.warn("Request to proxy {} failed, attempt {}/{}, error: {}",
                proxyAddr, i + 1, maxRetryTimes, e.getMessage());
        }
    }

    throw new ProxyException("All proxy addresses failed after " + maxRetryTimes + " attempts",
        lastException);
}
```

### 3. 重试机制关键特性

#### 3.1 配置参数

| 参数 | 默认值 | 作用 |
|-----|--------|------|
| `retryTimes` | 3 | 最大重试次数 |
| `requestTimeoutMillis` | 3000ms | 每次请求的超时时间 |
| `connectTimeoutMillis` | 3000ms | 连接建立超时时间 |
| `faultIsolationDurationMillis` | 30000ms（30秒） | 故障地址隔离时间 |

#### 3.2 故障隔离机制（ProxyAddressManager）

- **标记故障**：失败的代理地址会被标记，记录失败时间戳
- **轮询选择**：通过原子计数器实现Round-Robin地址选择
- **跳过故障地址**：选择地址时跳过处于隔离期的故障地址
- **自动恢复**：隔离时间超过`faultIsolationDurationMillis`后，地址自动恢复可用状态

```java
public String selectProxyAddr() {
    for (int i = 0; i < proxyAddrList.size(); i++) {
        int currentIndex = Math.abs(index.incrementAndGet()) % proxyAddrList.size();
        String addr = proxyAddrList.get(currentIndex);

        if (!isInFaultIsolation(addr)) {
            return addr;
        }
    }

    return null;  // 所有地址都被隔离
}
```

#### 3.3 通道管理

- **通道创建**：每次重试尝试创建新的通道连接
- **失败关闭**：请求失败后立即关闭对应通道，确保下次重试创建新连接
- **空闲清理**：定期扫描并关闭长时间未使用的通道

### 4. 各层级的重试责任

| 层级 | 类 | 是否重试 | 处理方式 |
|-----|---|---------|---------|
| SDK客户端 | `ProxyClient.send` | ❌ 否 | 委托给Facade的invokeSync |
| SDK重试循环 | `ProxyClientFacade.invokeSync` | ✅ **是** | 最多retryTimes次，故障隔离+轮询 |
| 代理处理器 | `SendMessageProcessor.processRequest` | ❌ 否 | 返回失败响应给SDK |
| 存储适配器 | `RocketMQStorageAdapter.putMessage` | ❌ 否 | 单次调用RocketMQ Broker |
| 代理内部网络客户端 | `NettyRemotingClient.invokeSync` | ❌ 否 | 单次发送+超时等待 |

### 5. 与原生RocketMQ客户端对比

**原生RocketMQ客户端（DefaultMQProducer）的重试行为：**
- 有`retryTimesWhenSendFailed`参数控制重试次数
- 有`retryAnotherBrokerWhenNotStoreOK`参数控制失败时切换Broker

**Proxy SDK的差异：**
- 不使用`DefaultMQProducer`，而是通过自定义协议直接与Broker通信
- 重试逻辑完全在SDK端实现，不依赖RocketMQ客户端的重试机制
- 增加了**故障隔离**机制，失败的代理地址会在30秒内被排除

## 二、单元测试验证

### 测试文件
`ProxyClientFacadeTest.java` - 包含12个测试场景

### 测试覆盖场景

#### 场景1: 成功无重试 ✅
- 第一次请求成功，不触发重试
- 验证只调用一次网络请求
- 验证不关闭通道

#### 场景2: 第一次失败后重试成功 ✅
- 第一次失败，第二次成功
- 验证调用两次网络请求
- 验证第一次失败后关闭通道

#### 场景3: 所有重试都失败 ✅
- 所有3次重试都失败
- 验证抛出ProxyException
- 验证异常消息包含重试次数信息
- 验证每次失败都关闭通道

#### 场景4: 故障隔离机制 ✅
- proxy1失败后，proxy2成功
- 第二次请求时proxy1被隔离，直接使用其他地址
- 验证故障隔离生效

#### 场景5: 轮询选择地址 ✅
- 连续3次请求选择不同的代理地址
- 验证Round-Robin轮询机制

#### 场景6: 无可用地址异常 ✅
- 只有一个代理地址且失败
- 验证抛出ProxyConnectException
- 验证异常消息包含"No available proxy address"

#### 场景7: 故障隔离超时后恢复 ✅
- 设置100ms短隔离时间
- proxy1失败后被隔离
- 150ms后proxy1恢复可用
- 验证自动恢复机制

#### 场景8: 重试次数可配置 ✅
- 配置重试次数为5次
- 配置5个代理地址
- 验证尝试5次重试

#### 场景9: 并发重试安全性 ✅
- 10个线程并发调用
- 所有线程成功完成
- 验证并发安全

#### 场景10: 部分失败后最终成功 ✅
- proxy1和proxy2失败，proxy3成功
- 验证在第3次重试成功
- 验证前两次失败关闭通道

#### 场景11: 请求失败时关闭通道 ✅
- 请求失败时验证通道被关闭

#### 场景12: 请求成功时不关闭通道 ✅
- 请求成功时验证通道不关闭

### 测试执行结果

```
Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

所有测试场景全部通过，验证了重试机制的以下关键特性：
1. ✅ 基本重试流程正常工作
2. ✅ 故障隔离机制生效
3. ✅ 地址轮询机制正常
4. ✅ 无可用地址时正确抛出异常
5. ✅ 故障隔离超时后自动恢复
6. ✅ 重试次数可配置
7. ✅ 并发调用安全
8. ✅ 通道管理正确（失败关闭，成功保留）

## 三、结论与建议

### 结论

Proxy SDK的重试机制设计完善，实现了以下核心功能：

1. **智能重试**：自动重试失败的请求，最多retryTimes次
2. **故障隔离**：失败的代理地址被自动隔离30秒，避免重复尝试失败节点
3. **地址轮询**：Round-Robin方式选择代理地址，实现负载均衡
4. **自动恢复**：隔离时间超过后，故障地址自动恢复可用
5. **连接管理**：失败时关闭连接，成功时保留连接，空闲时定期清理
6. **监控指标**：记录每次请求的成功/失败和耗时

### 建议

#### 使用建议

1. **配置多个代理地址**：建议至少配置2-3个代理地址，以实现故障转移
   ```java
   config.setProxyAddrs("proxy1:10911;proxy2:10912;proxy3:10913");
   ```

2. **根据业务场景调整重试次数**：
   - 关键业务：增加重试次数（如5次）
   - 非关键业务：减少重试次数（如2次）以降低延迟

3. **调整超时时间**：
   - 内网环境：可适当减少超时（如2000ms）
   - 跨网环境：增加超时（如5000ms）

#### 监控建议

1. **关注监控指标**：通过`client.getMetrics()`获取成功率和延迟数据
2. **日志监控**：观察WARN级别日志，了解重试频率和失败原因
3. **告警阈值**：建议设置失败率>20%时告警

#### 优化方向

1. **重试间隔**：当前实现无重试间隔，可考虑增加指数退避策略
2. **熔断机制**：可考虑实现更完善的熔断器模式（如Hystrix）
3. **动态配置**：支持运行时动态调整重试次数和超时时间

## 四、测试代码示例

### 单元测试示例

```java
@Test
public void testRetryAfterFirstFailure() throws Exception {
    // Given: 第一次失败，第二次成功
    RemotingCommand request = createTestRequest();
    RemotingCommand expectedResponse = createSuccessResponse();

    when(mockChannelManager.getOrCreateChannel(anyString()))
        .thenReturn(mockChannel1)  // 第一次
        .thenReturn(mockChannel2);  // 第二次

    when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
        .thenThrow(new RuntimeException("Connection refused"));
    when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
        .thenReturn(expectedResponse);

    // When: 调用 invokeSync
    RemotingCommand response = facade.invokeSync(request, 3000);

    // Then: 应该成功，调用了两次
    assertNotNull(response);
    assertEquals(expectedResponse, response);
    verify(mockRemotingClient, times(2)).invokeSync(any(Channel.class), eq(request), anyLong());
}
```

### 实际使用示例

```java
ProxyClientConfig config = new ProxyClientConfig();
config.setProxyAddrs("proxy1:10911;proxy2:10912;proxy3:10913");
config.setRetryTimes(3);
config.setRequestTimeoutMillis(3000);
config.setFaultIsolationDurationMillis(30000);

ProxyClient client = new ProxyClient(config);
client.start();

try {
    SendResult result = client.send("TestTopic", "TestTag", "Hello".getBytes());
    if (result.isSuccess()) {
        System.out.println("发送成功: " + result.getMsgId());
    } else {
        System.out.println("发送失败: " + result.getErrorMsg());
    }
} catch (ProxyException e) {
    System.out.println("重试失败: " + e.getMessage());
}

client.shutdown();
```

## 五、关键文件位置

- 重试核心实现：`mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyClientFacade.java`
- 地址管理：`mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyAddressManager.java`
- 通道管理：`mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/facade/ProxyChannelManager.java`
- 配置类：`mq-proxy-sdk/src/main/java/com/mq/proxy/sdk/client/ProxyClientConfig.java`
- 单元测试：`mq-proxy-sdk/src/test/java/com/mq/proxy/sdk/facade/ProxyClientFacadeTest.java`

---

**测试时间：2026-05-22**
**测试状态：全部通过（12/12）**
**结论：重试机制功能正常，符合设计预期**