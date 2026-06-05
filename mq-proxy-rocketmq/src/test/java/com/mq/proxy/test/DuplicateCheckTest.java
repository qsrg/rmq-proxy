package com.mq.proxy.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DuplicateCheckTest {

    private static final String PROXY_ADDR = System.getProperty("proxy.addr", "127.0.0.1:10913");
    private static final String TOPIC = "DupCheckTopic";
    private static final String PRODUCER_GROUP = "DupCheckProducerGroup";
    private static final String CONSUMER_GROUP = "DupCheckConsumerGroup";

    public static void main(String[] args) throws Exception {
        System.out.println("=== 重复消费检测测试 ===");
        System.out.println("Proxy地址: " + PROXY_ADDR);
        System.out.println();

        // 先启动consumer
        System.out.println("【步骤1】启动Consumer");
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(PROXY_ADDR);
        consumer.setConsumeFromWhere(org.apache.rocketmq.common.consumer.ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC, "*");

        AtomicInteger receivedCount = new AtomicInteger(0);
        ConcurrentHashMap<String, AtomicInteger> msgIdCountMap = new ConcurrentHashMap<>();

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    int count = receivedCount.incrementAndGet();
                    msgIdCountMap.computeIfAbsent(msg.getMsgId(), k -> new AtomicInteger(0)).incrementAndGet();
                    try {
                        System.out.println("  收到消息 #" + count + ": msgId=" + msg.getMsgId()
                            + ", queueId=" + msg.getQueueId()
                            + ", offset=" + msg.getQueueOffset()
                            + ", body=" + new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer已启动，等待3秒...");
        Thread.sleep(3000);

        // 发送消息
        System.out.println("\n【步骤2】发送5条消息");
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(PROXY_ADDR);
        producer.setRetryTimesWhenSendFailed(0);
        producer.setSendMsgTimeout(5000);
        producer.start();

        int sendCount = 5;
        for (int i = 0; i < sendCount; i++) {
            Message msg = new Message(TOPIC, "TagA",
                ("DupTest-" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            try {
                SendResult result = producer.send(msg);
                System.out.println("  发送消息 " + i + ": " + result.getSendStatus()
                    + ", msgId=" + result.getMsgId()
                    + ", queueId=" + result.getMessageQueue().getQueueId()
                    + ", offset=" + result.getQueueOffset());
            } catch (Exception e) {
                System.out.println("  发送消息 " + i + " 失败: " + e.getMessage());
            }
        }
        producer.shutdown();
        System.out.println("Producer已关闭");

        // 等待消费
        System.out.println("\n【步骤3】等待20秒消费消息...");
        Thread.sleep(20000);

        consumer.shutdown();

        // 统计结果
        System.out.println("\n=== 测试结果 ===");
        System.out.println("发送消息数: " + sendCount);
        System.out.println("收到消息总数: " + receivedCount.get());
        System.out.println("唯一消息数: " + msgIdCountMap.size());

        boolean hasDuplicate = false;
        for (ConcurrentHashMap.Entry<String, AtomicInteger> entry : msgIdCountMap.entrySet()) {
            if (entry.getValue().get() > 1) {
                hasDuplicate = true;
                System.out.println("重复消息: msgId=" + entry.getKey() + ", 次数=" + entry.getValue().get());
            }
        }

        if (hasDuplicate) {
            System.out.println("\n!!! 存在重复消费问题 !!!");
        } else if (receivedCount.get() == sendCount && msgIdCountMap.size() == sendCount) {
            System.out.println("\n✓ 测试通过：消息收发正常，无重复消费");
        } else if (receivedCount.get() < sendCount) {
            System.out.println("\n!!! 消息丢失：只收到 " + receivedCount.get() + " 条消息 !!!");
        } else {
            System.out.println("\n!!! 消息数量异常：收到 " + receivedCount.get() + " 条消息（含旧消息）!!!");
        }
    }
}
