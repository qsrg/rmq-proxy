package com.mq.proxy.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NativeBrokerDistributionTest {

    private static final String PROXY_ADDR = System.getProperty("proxy.addr", "127.0.0.1:10916");
    private static final String TOPIC = System.getProperty("test.topic", "BrokerDistributionTopic");
    private static final int MESSAGE_COUNT = Integer.getInteger("test.messageCount", 16);
    private static final int EXPECTED_MIN_BROKERS = Integer.getInteger("test.expectedMinBrokers", 2);

    public static void main(String[] args) throws Exception {
        String runId = Long.toHexString(System.currentTimeMillis());
        String producerGroup = "DistProducer_" + runId;
        String consumerGroup = "DistConsumer_" + runId;

        System.out.println("=== Native Broker Distribution Test ===");
        System.out.println("Proxy: " + PROXY_ADDR);
        System.out.println("Topic: " + TOPIC);
        System.out.println("MessageCount: " + MESSAGE_COUNT);
        System.out.println("ExpectedMinBrokers: " + EXPECTED_MIN_BROKERS);

        CountDownLatch receiveLatch = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger receivedCount = new AtomicInteger(0);
        Set<String> consumedBodies = ConcurrentHashMap.newKeySet();
        Set<String> sendBrokerNames = ConcurrentHashMap.newKeySet();
        Set<String> consumeBrokerNames = ConcurrentHashMap.newKeySet();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(PROXY_ADDR);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.subscribe(TOPIC, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        int current = receivedCount.incrementAndGet();
                        consumedBodies.add(body);
                        consumeBrokerNames.add(msg.getBrokerName());
                        System.out.println("RECV #" + current
                                + " broker=" + msg.getBrokerName()
                                + " queueId=" + msg.getQueueId()
                                + " offset=" + msg.getQueueOffset()
                                + " body=" + body);
                        receiveLatch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();

        Thread.sleep(3000L);

        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(PROXY_ADDR);
        producer.setRetryTimesWhenSendFailed(0);
        producer.setSendMsgTimeout(5000);
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String body = "dist-" + runId + "-" + i + "-" + UUID.randomUUID();
            Message msg = new Message(TOPIC, "TagA", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            String brokerName = result.getMessageQueue().getBrokerName();
            sendBrokerNames.add(brokerName);
            System.out.println("SEND #" + i
                    + " broker=" + brokerName
                    + " queueId=" + result.getMessageQueue().getQueueId()
                    + " offset=" + result.getQueueOffset()
                    + " msgId=" + result.getMsgId()
                    + " body=" + body);
        }

        boolean completed = receiveLatch.await(30, TimeUnit.SECONDS);

        producer.shutdown();
        consumer.shutdown();

        System.out.println("RESULT completed=" + completed
                + " received=" + receivedCount.get()
                + " uniqueBodies=" + consumedBodies.size()
                + " sendBrokers=" + sendBrokerNames
                + " consumeBrokers=" + consumeBrokerNames);

        if (!completed || consumedBodies.size() != MESSAGE_COUNT) {
            throw new IllegalStateException("Not all messages were consumed");
        }
        if (sendBrokerNames.size() < EXPECTED_MIN_BROKERS) {
            throw new IllegalStateException("Messages were not distributed across enough brokers: " + sendBrokerNames);
        }
    }
}
