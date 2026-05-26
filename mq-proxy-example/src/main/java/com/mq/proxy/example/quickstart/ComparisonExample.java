package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

/**
 * Proxy SDK vs 原生 RocketMQ 对比示例
 *
 * 展示两种客户端的使用差异和特点对比
 */
public class ComparisonExample {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  Proxy SDK vs 原生 RocketMQ 对比");
        System.out.println("========================================");
        System.out.println();

        String topic = "ComparisonTopic";
        String tags = "TagA";
        String keys = "ComparisonKey";
        String messageBody = "Hello World!";

        // ==================== Proxy SDK ====================
        System.out.println("【方案1】使用 Proxy SDK");
        System.out.println();

        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("ProxySDKGroup");
        config.setRetryTimes(3);
        config.setFaultIsolationDurationMillis(30000);

        ProxyClient proxyClient = new ProxyClient(config);

        try {
            proxyClient.start();

            System.out.println("代码示例:");
            System.out.println("  ProxyClientConfig config = new ProxyClientConfig();");
            System.out.println("  config.setProxyAddrs(\"127.0.0.1:11911\");");
            System.out.println("  ProxyClient client = new ProxyClient(config);");
            System.out.println("  client.start();");
            System.out.println();
            System.out.println("  SendResult result = client.send(topic, tags, keys.getBytes(), body.getBytes());");
            System.out.println();

            SendResult proxyResult = proxyClient.send(
                topic,
                tags,
                keys,  // String类型
                messageBody.getBytes()
            );

            System.out.println("发送结果:");
            System.out.println("  成功: " + proxyResult.isSuccess());
            System.out.println("  消息ID: " + proxyResult.getMsgId());
            System.out.println("  队列ID: " + proxyResult.getQueueId());
            System.out.println("  队列偏移量: " + proxyResult.getQueueOffset());
            System.out.println("  TraceID: " + proxyResult.getTraceId());
            System.out.println();

            System.out.println("Proxy SDK 特点:");
            System.out.println("  ✓ 通过代理层访问RocketMQ");
            System.out.println("  ✓ 内置故障隔离机制");
            System.out.println("  ✓ Round-Robin地址轮询");
            System.out.println("  ✓ 自动重试+故障转移");
            System.out.println("  ✓ 详细的监控指标");
            System.out.println("  ✓ 支持链路追踪");
            System.out.println("  ✓ 统一的协议层抽象");
            System.out.println();

        } catch (Exception e) {
            System.out.println("Proxy SDK 异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            proxyClient.shutdown();
        }

        System.out.println();

        // ==================== 原生 RocketMQ ====================
        System.out.println("【方案2】使用原生 RocketMQ 客户端");
        System.out.println("（通过Proxy代理访问RocketMQ）");
        System.out.println();

        DefaultMQProducer rocketmqProducer = new DefaultMQProducer("RocketMQGroup");
        rocketmqProducer.setNamesrvAddr("127.0.0.1:11911");  // 配置Proxy地址作为NameServer
        rocketmqProducer.setRetryTimesWhenSendFailed(3);

        try {
            rocketmqProducer.start();

            System.out.println("代码示例:");
            System.out.println("  DefaultMQProducer producer = new DefaultMQProducer(group);");
            System.out.println("  producer.setNamesrvAddr(\"127.0.0.1:11911\");  // Proxy地址");
            System.out.println("  producer.start();");
            System.out.println();
            System.out.println("  Message msg = new Message(topic, tags, keys, body.getBytes());");
            System.out.println("  SendResult result = producer.send(msg);");
            System.out.println();

            Message msg = new Message(topic, tags, keys, messageBody.getBytes());
            org.apache.rocketmq.client.producer.SendResult rocketmqResult =
                rocketmqProducer.send(msg);

            System.out.println("发送结果:");
            System.out.println("  消息ID: " + rocketmqResult.getMsgId());
            System.out.println("  发送状态: " + rocketmqResult.getSendStatus());
            System.out.println("  消息队列: " + rocketmqResult.getMessageQueue());
            System.out.println("  队列偏移量: " + rocketmqResult.getQueueOffset());
            System.out.println();

            System.out.println("原生 RocketMQ 特点:");
            System.out.println("  ✓ 通过Proxy代理访问RocketMQ");
            System.out.println("  ✓ 配置Proxy地址作为NameServer");
            System.out.println("  ✓ 使用RocketMQ原生协议和API");
            System.out.println("  ✓ 支持事务消息、延迟消息等高级特性");
            System.out.println("  ✓ 丰富的消息类型");
            System.out.println("  ✓ 成熟的生态体系");
            System.out.println();
            System.out.println("  ✓ 内置重试机制");
            System.out.println("  ✓ 支持事务消息");
            System.out.println("  ✓ 支持延迟消息");
            System.out.println("  ✓ 丰富的消息类型");
            System.out.println("  ✓ 成熟的生态体系");
            System.out.println();

        } catch (MQClientException e) {
            System.out.println("RocketMQ 客户端异常: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("RocketMQ 发送异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            rocketmqProducer.shutdown();
        }

        System.out.println();

        // ==================== 对比总结 ====================
        System.out.println("========================================");
        System.out.println("  对比总结");
        System.out.println("========================================");
        System.out.println();

        System.out.println("架构差异:");
        System.out.println();
        System.out.println("Proxy SDK:");
        System.out.println("  Producer → Proxy SDK → Proxy Agent → RocketMQ Broker");
        System.out.println("  - 使用自定义SDK协议");
        System.out.println("  - 配置Proxy地址（proxyAddrs）");
        System.out.println();
        System.out.println("原生 RocketMQ:");
        System.out.println("  Producer → RocketMQ Client → Proxy Agent → RocketMQ Broker");
        System.out.println("  - 使用RocketMQ原生协议");
        System.out.println("  - 配置Proxy地址作为NameServer（namesrvAddr）");
        System.out.println("  - Proxy充当NameServer代理");
        System.out.println();

        System.out.println("使用场景:");
        System.out.println();
        System.out.println("推荐使用 Proxy SDK:");
        System.out.println("  ✓ 需要统一的协议层管理");
        System.out.println("  ✓ 需要强大的故障隔离能力");
        System.out.println("  ✓ 需要详细的监控和追踪");
        System.out.println("  ✓ 需要快速切换底层存储");
        System.out.println("  ✓ 多租户/多集群场景");
        System.out.println("  ✓ 自定义重试策略");
        System.out.println();
        System.out.println("推荐使用原生 RocketMQ:");
        System.out.println("  ✓ 已有RocketMQ代码基础");
        System.out.println("  ✓ 需要事务消息、延迟消息等高级特性");
        System.out.println("  ✓ 使用RocketMQ成熟生态");
        System.out.println("  ✓ 最小化迁移成本");
        System.out.println("  ✓ 兼容现有RocketMQ工具和运维");
        System.out.println();

        System.out.println("配置差异:");
        System.out.println();
        System.out.println("Proxy SDK:");
        System.out.println("  - setProxyAddrs(\"proxy1:11911;proxy2:11912\")");
        System.out.println("  - 自定义重试次数、故障隔离时间");
        System.out.println("  - 启用监控和追踪功能");
        System.out.println();
        System.out.println("原生 RocketMQ:");
        System.out.println("  - setNamesrvAddr(\"proxy1:11911;proxy2:11912\")  // 也是Proxy地址");
        System.out.println("  - RocketMQ标准重试配置");
        System.out.println("  - 使用RocketMQ标准超时和实例配置");
        System.out.println();

        System.out.println("========================================");
        System.out.println("选择建议:");
        System.out.println();
        System.out.println("两种方式都通过Proxy访问RocketMQ，获得代理层的优势:");
        System.out.println("  - 协议统一管理");
        System.out.println("  - 虚拟路由管理");
        System.out.println("  - 安全隔离");
        System.out.println();
        System.out.println("Proxy SDK提供额外的功能:");
        System.out.println("  - 自定义重试和故障隔离");
        System.out.println("  - 详细监控指标");
        System.out.println("  - 链路追踪");
        System.out.println();
        System.out.println("原生RocketMQ提供:");
        System.out.println("  - RocketMQ所有高级特性");
        System.out.println("  - 最小迁移成本");
        System.out.println("  - 成熟生态工具");
        System.out.println("========================================");
    }
}