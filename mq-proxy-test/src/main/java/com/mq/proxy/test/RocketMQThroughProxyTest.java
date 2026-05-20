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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RocketMQThroughProxyTest {
    
    private static final String PROXY_ADDR = "127.0.0.1:10913";
    private static final String TOPIC = "TestTopic";
    private static final String PRODUCER_GROUP = "TestProducerGroup";
    private static final String CONSUMER_GROUP = "TestConsumerGroup";
    
    private static final AtomicInteger receivedCount = new AtomicInteger(0);
    private static final CountDownLatch latch = new CountDownLatch(10);
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== RocketMQ 通过 Proxy 消息收发测试 ===");
        System.out.println();
        System.out.println("配置信息:");
        System.out.println("  NameServer: 127.0.0.1:9876");
        System.out.println("  Broker: 127.0.0.1:10911");
        System.out.println("  Proxy: " + PROXY_ADDR);
        System.out.println("  Topic: " + TOPIC);
        System.out.println();
        
        Thread consumerThread = new Thread(() -> {
            try {
                testConsumer();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        
        consumerThread.start();
        
        System.out.println("等待 Consumer 初始化完成...");
        Thread.sleep(5000);
        System.out.println();
        
        testProducer();
        
        System.out.println("等待消费消息...");
        boolean allReceived = latch.await(60, TimeUnit.SECONDS);
        
        if (allReceived) {
            System.out.println("✓ 所有消息已消费完成，共收到 " + receivedCount.get() + " 条消息");
        } else {
            System.out.println("⚠ 等待超时，已收到 " + receivedCount.get() + " 条消息");
        }
        
        System.out.println();
        Thread.sleep(2000);
        
        consumerThread.interrupt();
        
        System.out.println("=== 测试完成 ===");
    }
    
    private static void testProducer() throws Exception {
        System.out.println("【步骤1】创建 Producer，连接 Proxy");
        System.out.println("  注意：Producer 的 namesrvAddr 设置为 Proxy 地址");
        System.out.println();
        
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(PROXY_ADDR);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        
        producer.start();
        System.out.println("✓ Producer 启动成功");
        System.out.println();
        
        System.out.println("【步骤2】发送 10 条测试消息");
        for (int i = 0; i < 10; i++) {
            Message msg = new Message(TOPIC, "TagA", 
                ("Hello RocketMQ through Proxy - Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            
            SendResult result = producer.send(msg);
            System.out.println("消息 " + i + ": 发送成功");
            System.out.println("  msgId: " + result.getMsgId());
            System.out.println("  sendStatus: " + result.getSendStatus());
            System.out.println("  queueId: " + result.getMessageQueue().getQueueId());
            System.out.println("  queueOffset: " + result.getQueueOffset());
        }
        
        producer.shutdown();
        System.out.println();
        System.out.println("✓ Producer 已关闭");
        System.out.println();
    }
    
    private static void testConsumer() throws Exception {
        System.out.println("【步骤3】创建 Consumer，连接 Proxy");
        System.out.println("  注意：Consumer 的 namesrvAddr 设置为 Proxy 地址");
        System.out.println();
        
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(PROXY_ADDR);
        consumer.subscribe(TOPIC, "TagA");
        
        System.out.println("✓ Consumer 启动中...");
        
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    int count = receivedCount.incrementAndGet();
                    System.out.println("✓ 收到消息 #" + count + ":");
                    System.out.println("  msgId: " + msg.getMsgId());
                    System.out.println("  topic: " + msg.getTopic());
                    System.out.println("  tags: " + msg.getTags());
                    try {
                        System.out.println("  body: " + new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("  queueId: " + msg.getQueueId());
                    System.out.println("  queueOffset: " + msg.getQueueOffset());
                    System.out.println();
                    
                    latch.countDown();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        
        consumer.start();
        System.out.println("✓ Consumer 已启动，等待 Rebalance 和消息拉取...");
        System.out.println();
        
        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(1000);
        }
        
        consumer.shutdown();
        System.out.println("✓ Consumer 已关闭");
    }
}