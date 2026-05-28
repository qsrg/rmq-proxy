package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.PullResult;
import com.mq.proxy.sdk.exception.ProxyException;

/**
 * Proxy SDK 消费者快速开始示例 - 使用简化API
 */
public class ProxySDKConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  Proxy SDK 消费者快速开始示例");
        System.out.println("========================================");
        System.out.println();

        // 创建Consumer（类似RocketMQ）
        ProxyConsumer consumer = new ProxyConsumer("ConsumerGroup")
            .setProxyAddrs("127.0.0.1:11911")
            .setRetryTimes(3)
            .setEnableMetrics(true);

        try {
            consumer.start();
            System.out.println("✓ Consumer 启动成功");
            System.out.println();

            // 消费参数
            String topic = "QuickStartTopic";
            String consumerGroup = "ConsumerGroup";
            int queueId = 0;
            long offset = 0;
            int maxNums = 32;

            System.out.println("消费配置:");
            System.out.println("  Topic: " + topic);
            System.out.println("  ConsumerGroup: " + consumerGroup);
            System.out.println("  QueueId: " + queueId);
            System.out.println("  Offset: " + offset);
            System.out.println("  MaxNums: " + maxNums);
            System.out.println();

            System.out.println("开始消费消息...");
            System.out.println();

            int totalConsumed = 0;
            int round = 0;
            int maxRounds = 10;

            while (round < maxRounds) {
                System.out.println("--- 消费轮次 #" + round + " ---");

                try {
                    PullResult pullResult = consumer.pull(topic, consumerGroup, queueId, offset, maxNums);

                    if (pullResult != null && pullResult.isFound()) {
                        byte[] body = pullResult.getBody();

                        if (body != null && body.length > 0) {
                            String messageContent = new String(body);
                            System.out.println("✓ 拉取成功:");
                            System.out.println("  内容: " + messageContent);
                            System.out.println("  NextOffset: " + pullResult.getNextBeginOffset());

                            offset = pullResult.getNextBeginOffset();
                            totalConsumed++;

                            System.out.println("  处理消息...");
                            Thread.sleep(100);
                        } else {
                            System.out.println("没有新消息");
                        }
                    } else {
                        System.out.println("没有新消息");
                    }

                } catch (ProxyException e) {
                    System.out.println("拉取异常: " + e.getMessage());
                }

                System.out.println();
                round++;
                Thread.sleep(500);
            }

            System.out.println("========================================");
            System.out.println("消费统计:");
            System.out.println("  总消费轮数: " + maxRounds);
            System.out.println("  成功消费: " + totalConsumed + " 条");
            System.out.println("  当前偏移量: " + offset);
            System.out.println();
            System.out.println("✓ 消费示例完成");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.shutdown();
            System.out.println();
            System.out.println("✓ Consumer 已关闭");
        }
    }
}
