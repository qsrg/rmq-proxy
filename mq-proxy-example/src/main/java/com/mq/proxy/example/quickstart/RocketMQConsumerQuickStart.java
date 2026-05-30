package com.mq.proxy.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 原生 RocketMQ 消费者快速开始示例
 *
 * 展示使用原生RocketMQ Push客户端通过Proxy消费消息
 */
public class RocketMQConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  原生 RocketMQ 消费者快速开始示例");
        System.out.println("========================================");
        System.out.println();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ConsumerGroup");
        consumer.setNamesrvAddr("127.0.0.1:19876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try {
            String topic = "QuickStartTopic";
            consumer.subscribe(topic, "*");

            System.out.println("消费配置:");
            System.out.println("  Topic: " + topic);
            System.out.println("  ConsumerGroup: " + consumer.getConsumerGroup());
            System.out.println("  NamesrvAddr: " + consumer.getNamesrvAddr() + " (Proxy地址)");
            System.out.println("  ConsumeFromWhere: CONSUME_FROM_FIRST_OFFSET");
            System.out.println();

            AtomicInteger totalConsumed = new AtomicInteger(0);

            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        int count = totalConsumed.incrementAndGet();
                        String body = new String(msg.getBody());
                        System.out.println("消费消息 #" + count + ":");
                        System.out.println("  MsgId: " + msg.getMsgId());
                        System.out.println("  内容: " + body);
                        System.out.println("  QueueId: " + msg.getQueueId() + ", QueueOffset: " + msg.getQueueOffset());
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
            System.out.println("✓ Consumer 启动成功，等待消息...");
            System.out.println();

            Thread.sleep(30000);

            System.out.println();
            System.out.println("========================================");
            System.out.println("消费统计:");
            System.out.println("  成功消费: " + totalConsumed.get() + " 条");
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
