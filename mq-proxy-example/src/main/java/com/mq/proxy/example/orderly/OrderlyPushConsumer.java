package com.mq.proxy.example.orderly;

import com.mq.proxy.sdk.consumer.model.ProxyMessage;
import com.mq.proxy.sdk.consumer.push.ConsumeStatus;
import com.mq.proxy.sdk.consumer.push.ProxyMessageListener;
import com.mq.proxy.sdk.consumer.push.ProxyPushConsumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderlyPushConsumer {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  顺序消息 Push Consumer 示例");
        System.out.println("========================================");
        System.out.println();
        System.out.println("说明: subscribe 后自动 rebalance 分配队列");
        System.out.println("      只需注册 Listener 处理消息即可");
        System.out.println();

        ProxyPushConsumer consumer = new ProxyPushConsumer("OrderlyPushConsumerGroup")
            .setProxyAddrs("127.0.0.1:19876")
            .setRetryTimes(3);

        consumer.subscribe("OrderlyTopic", "*");

        AtomicInteger counter = new AtomicInteger(0);

        consumer.registerMessageListener(new ProxyMessageListener() {
            @Override
            public ConsumeStatus consume(List<ProxyMessage> messages) {
                for (ProxyMessage msg : messages) {
                    int count = counter.incrementAndGet();
                    System.out.println("消费消息[" + count + "]: " + new String(msg.getBody())
                        + ", queueId=" + msg.getQueueId()
                        + ", offset=" + msg.getQueueOffset());
                }
                return ConsumeStatus.SUCCESS;
            }
        });

        try {
            consumer.start();
            System.out.println("PushConsumer 启动成功，等待消息...");
            System.out.println();

            Thread.sleep(30000);

            System.out.println();
            System.out.println("========================================");
            System.out.println("消费完成，总计: " + counter.get() + " 条");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("异常: " + e.getMessage());
        } finally {
            consumer.shutdown();
            System.out.println("PushConsumer 已关闭");
        }
    }
}