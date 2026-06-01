package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.consumer.model.ProxyMessage;
import com.mq.proxy.sdk.consumer.push.ConsumeStatus;
import com.mq.proxy.sdk.consumer.push.ProxyMessageListener;
import com.mq.proxy.sdk.consumer.push.ProxyPushConsumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyPushConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  Proxy Push Consumer 快速开始示例");
        System.out.println("========================================");
        System.out.println();

        ProxyPushConsumer consumer = new ProxyPushConsumer("PushConsumerGroup")
            .setProxyAddrs("127.0.0.1:19876")
            .setRetryTimes(3);

        consumer.subscribe("QuickStartTopic", "*");

        AtomicInteger counter = new AtomicInteger(0);

        consumer.registerMessageListener(new ProxyMessageListener() {
            @Override
            public ConsumeStatus consume(List<ProxyMessage> messages) {
                for (ProxyMessage msg : messages) {
                    int count = counter.incrementAndGet();
                    String body = new String(msg.getBody());
                    System.out.println("✓ 消费消息[" + count + "]: " + body
                        + ", queueId=" + msg.getQueueId()
                        + ", offset=" + msg.getQueueOffset());
                }
                return ConsumeStatus.SUCCESS;
            }
        });

        try {
            consumer.start();
            System.out.println("PushConsumer 启动成功，等待消息（自动 rebalance + offset 管理）...");
            System.out.println();

            Thread.sleep(60000);

            System.out.println();
            System.out.println("========================================");
            System.out.println("消费完成，总计: " + counter.get() + " 条");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("异常: " + e.getMessage());
        } finally {
            //consumer.shutdown();
            //System.out.println("PushConsumer 已关闭");
        }
    }
}