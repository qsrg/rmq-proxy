package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.PullResult;
import com.mq.proxy.sdk.exception.ProxyException;

/**
 * Proxy SDK 消费者快速开始示例
 *
 * 展示使用Proxy SDK拉取消息的完整流程
 */
public class ProxySDKConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  Proxy SDK 消费者快速开始示例");
        System.out.println("========================================");
        System.out.println();

        // 创建配置
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("ConsumerGroup");  // 消费者组名
        config.setSuspendTimeoutMillis(15000);  // 长轮询超时15秒
        config.setRetryTimes(3);
        config.setEnableMetrics(true);

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ Consumer 启动成功");
            System.out.println();

            // 消费参数
            String topic = "QuickStartTopic";
            String consumerGroup = "ConsumerGroup";
            int queueId = 0;  // 队列ID
            long offset = 0;  // 从偏移量0开始
            int maxNums = 32;  // 每次拉取32条

            System.out.println("消费配置:");
            System.out.println("  Topic: " + topic);
            System.out.println("  ConsumerGroup: " + consumerGroup);
            System.out.println("  QueueId: " + queueId);
            System.out.println("  Offset: " + offset);
            System.out.println("  MaxNums: " + maxNums);
            System.out.println("  长轮询超时: " + config.getSuspendTimeoutMillis() + " ms");
            System.out.println();

            System.out.println("开始消费消息（长轮询模式）...");
            System.out.println();

            int totalConsumed = 0;
            int round = 0;
            int maxRounds = 10;  // 消费10轮

            while (round < maxRounds) {
                System.out.println("--- 消费轮次 #" + round + " ---");

                try {
                    // 拉取消息
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
                            System.out.println("✓ 拉取成功:");
                            System.out.println("  内容: " + messageContent);
                            System.out.println("  NextOffset: " + pullResult.getNextBeginOffset());

                            // 更新偏移量
                            offset = pullResult.getNextBeginOffset();
                            totalConsumed++;

                            // 模拟业务处理
                            System.out.println("  处理消息...");
                            Thread.sleep(100);
                        } else {
                            System.out.println("没有新消息（长轮询超时）");
                        }
                    } else {
                        System.out.println("没有新消息");
                    }

                } catch (ProxyException e) {
                    System.out.println("拉取异常: " + e.getMessage());
                }

                System.out.println();
                round++;
                Thread.sleep(500);  // 模拟消费间隔
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
            client.shutdown();
            System.out.println();
            System.out.println("✓ Consumer 已关闭");
        }
    }
}