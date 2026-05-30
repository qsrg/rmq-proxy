package com.mq.proxy.example.orderly;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class OrderlyConsumer {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  顺序消息消费者示例 (原生RocketMQ客户端)");
        System.out.println("========================================");
        System.out.println();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("OrderlyConsumerGroup");
        consumer.setNamesrvAddr("127.0.0.1:19876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        try {
            consumer.subscribe("OrderlyTopic", "TagA || TagB");

            AtomicInteger counter = new AtomicInteger(0);

            consumer.registerMessageListener(new MessageListenerOrderly() {
                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                    context.setAutoCommit(true);
                    for (MessageExt msg : msgs) {
                        int count = counter.incrementAndGet();
                        System.out.println("消费消息: " + new String(msg.getBody())
                                + ", queueId=" + msg.getQueueId()
                                + ", offset=" + msg.getQueueOffset()
                                + ", reconsumeTimes=" + msg.getReconsumeTimes()
                                + ", 总计=" + count);
                    }
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            });

            consumer.start();
            System.out.println("Consumer 启动成功，等待消息...");
            System.out.println("顺序消费保证同一队列的消息按顺序处理");
            System.out.println();

            Thread.sleep(30000);

            System.out.println();
            System.out.println("========================================");
            System.out.println("消费完成，总计消费: " + counter.get() + " 条");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("消费异常: " + e.getMessage());
        } finally {
            consumer.shutdown();
            System.out.println("Consumer 已关闭");
        }
    }
}
