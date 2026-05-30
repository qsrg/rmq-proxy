package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.consumer.lite.ProxyLitePullConsumer;
import com.mq.proxy.sdk.consumer.model.ProxyMessage;

import java.util.List;

public class ProxyLitePullConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  Proxy Lite Pull Consumer 快速开始示例");
        System.out.println("========================================");
        System.out.println();

        ProxyLitePullConsumer consumer = new ProxyLitePullConsumer("LitePullConsumerGroup")
            .setProxyAddrs("127.0.0.1:19876")
            .setRetryTimes(3)
            .setPullBatchSize(32)
            .setAutoCommit(true);

        consumer.subscribe("QuickStartTopic", "*");

        try {
            consumer.start();
            System.out.println("LitePullConsumer 启动成功");
            System.out.println("已分配队列: " + consumer.assignment());
            System.out.println();

            int totalConsumed = 0;
            int maxRounds = 30;

            for (int round = 0; round < maxRounds; round++) {
                List<ProxyMessage> messages = consumer.poll(2000);

                if (messages.isEmpty()) {
                    System.out.println("轮次 " + round + ": 暂无消息");
                } else {
                    System.out.println("--- 轮次 " + round + ", 收到 " + messages.size() + " 条消息 ---");
                    for (ProxyMessage msg : messages) {
                        String body = new String(msg.getBody());
                        System.out.println("  ✓ " + body
                            + ", queueId=" + msg.getQueueId()
                            + ", offset=" + msg.getQueueOffset());
                        totalConsumed++;
                    }
                }
            }

            System.out.println();
            System.out.println("========================================");
            System.out.println("消费完成，总计: " + totalConsumed + " 条");
            System.out.println("无需手动管理 queueId 和 offset");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("异常: " + e.getMessage());
        } finally {
            consumer.shutdown();
            System.out.println("LitePullConsumer 已关闭");
        }
    }
}