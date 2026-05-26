package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.PullResult;
import com.mq.proxy.sdk.client.SendResult;

/**
 * 完整的生产-消费流程示例
 *
 * 展示从生产消息到消费消息的完整流程
 */
public class ProducerConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  完整的生产-消费流程示例");
        System.out.println("========================================");
        System.out.println();

        // 创建配置
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("ProducerConsumerGroup");
        config.setRetryTimes(3);
        config.setSuspendTimeoutMillis(10000);  // 长轮询10秒
        config.setEnableMetrics(true);

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ Client 启动成功");
            System.out.println();

            String topic = "QuickStartTopic";
            String tags = "TagA";
            String consumerGroup = "ProducerConsumerGroup";

            // ==================== 生产消息 ====================
            System.out.println("【阶段1】生产消息");
            System.out.println();

            int produceCount = 5;
            int successCount = 0;

            for (int i = 0; i < produceCount; i++) {
                String keys = "Key_" + i;
                String body = "Message #" + i + " - " + System.currentTimeMillis();

                SendResult sendResult = client.send(topic, tags, keys, body.getBytes());

                if (sendResult.isSuccess()) {
                    successCount++;
                    System.out.println("[" + i + "] ✓ 发送成功");
                    System.out.println("  MsgId: " + sendResult.getMsgId());
                    System.out.println("  QueueId: " + sendResult.getQueueId());
                    System.out.println("  Offset: " + sendResult.getQueueOffset());
                    System.out.println("  内容: " + body);
                } else {
                    System.out.println("[" + i + "] ✗ 发送失败: " + sendResult.getErrorMsg());
                }

                System.out.println();
                Thread.sleep(100);
            }

            System.out.println("生产统计:");
            System.out.println("  发送总数: " + produceCount);
            System.out.println("  成功数: " + successCount);
            System.out.println();

            // ==================== 消费消息 ====================
            System.out.println("【阶段2】消费消息");
            System.out.println("提示：需要等待生产者的消息被Broker处理");
            System.out.println();

            Thread.sleep(1000);  // 等待消息就绪

            int queueId = 0;
            long offset = 0;  // 从偏移量0开始
            int maxNums = 32;
            int consumedCount = 0;
            int consumeRounds = 10;

            System.out.println("开始消费...");

            for (int round = 0; round < consumeRounds; round++) {
                System.out.println("--- 消费轮次 #" + round + " ---");

                PullResult pullResult = client.pull(
                    topic,
                    consumerGroup,
                    queueId,
                    offset,
                    maxNums
                );

                if (pullResult != null && pullResult.isFound()) {
                    byte[] body = pullResult.getBody();

                    if (body != null && body.length > 0) {
                        String messageContent = new String(body);
                        System.out.println("✓ 消费成功:");
                        System.out.println("  内容: " + messageContent);
                        System.out.println("  NextOffset: " + pullResult.getNextBeginOffset());

                        consumedCount++;
                        offset = pullResult.getNextBeginOffset();

                        // 模拟业务处理
                        Thread.sleep(100);
                    } else {
                        System.out.println("没有新消息");
                    }
                } else {
                    System.out.println("没有新消息（可能已消费完毕）");
                }

                System.out.println();
                Thread.sleep(300);
            }

            System.out.println("消费统计:");
            System.out.println("  消费轮数: " + consumeRounds);
            System.out.println("  成功消费: " + consumedCount + " 条");
            System.out.println("  当前偏移量: " + offset);
            System.out.println();

            // ==================== 总结 ====================
            System.out.println("========================================");
            System.out.println("完整流程统计:");
            System.out.println("  生产成功: " + successCount + " 条");
            System.out.println("  消费成功: " + consumedCount + " 条");
            System.out.println();
            System.out.println("✓ 生产-消费流程完成");
            System.out.println("========================================");

            // 获取监控数据
            System.out.println();
            System.out.println("监控数据:");
            client.getMetrics().forEach((proxyAddr, snapshot) -> {
                System.out.println("代理: " + proxyAddr);
                System.out.println("  成功: " + snapshot.getSuccessCount());
                System.out.println("  失败: " + snapshot.getFailureCount());
                System.out.println("  成功率: " + String.format("%.2f%%", snapshot.getSuccessRate() * 100));
                System.out.println("  平均延迟: " + snapshot.getAvgElapsedMillis() + " ms");
            });

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println();
            System.out.println("✓ Client 已关闭");
        }
    }
}