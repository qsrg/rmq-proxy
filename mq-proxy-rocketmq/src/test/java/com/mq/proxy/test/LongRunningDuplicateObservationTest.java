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

public class LongRunningDuplicateObservationTest {

    private static final String PROXY_ADDR = System.getProperty("proxy.addr", "127.0.0.1:10913");
    private static final String TOPIC = System.getProperty("test.topic", "LongDupTopic");
    private static final int MESSAGE_COUNT = Integer.getInteger("test.messageCount", 5);
    private static final int OBSERVE_SECONDS = Integer.getInteger("test.observeSeconds", 120);

    public static void main(String[] args) throws Exception {
        String runId = Long.toHexString(System.currentTimeMillis());
        String producerGroup = "LongDupProducer_" + runId;
        String consumerGroup = "LongDupConsumer_" + runId;

        System.out.println("=== Long Running Duplicate Observation Test ===");
        System.out.println("Proxy: " + PROXY_ADDR);
        System.out.println("Topic: " + TOPIC);
        System.out.println("ProducerGroup: " + producerGroup);
        System.out.println("ConsumerGroup: " + consumerGroup);
        System.out.println("ObserveSeconds: " + OBSERVE_SECONDS);

        CountDownLatch firstBatchLatch = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger totalReceived = new AtomicInteger(0);
        Set<String> uniqueBodies = ConcurrentHashMap.newKeySet();
        ConcurrentHashMap<String, AtomicInteger> receiveCountByBody = new ConcurrentHashMap<>();

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
                        int seen = receiveCountByBody.computeIfAbsent(body, key -> new AtomicInteger()).incrementAndGet();
                        uniqueBodies.add(body);
                        int current = totalReceived.incrementAndGet();
                        System.out.println("RECV #" + current
                                + " seen=" + seen
                                + " msgId=" + msg.getMsgId()
                                + " broker=" + msg.getBrokerName()
                                + " queueId=" + msg.getQueueId()
                                + " offset=" + msg.getQueueOffset()
                                + " body=" + body);
                        if (seen > 1) {
                            System.out.println("DUPLICATE body=" + body + " seen=" + seen
                                    + " msgId=" + msg.getMsgId()
                                    + " queueId=" + msg.getQueueId()
                                    + " offset=" + msg.getQueueOffset());
                        }
                        firstBatchLatch.countDown();
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
            String body = "longdup-" + runId + "-" + i + "-" + UUID.randomUUID();
            Message msg = new Message(TOPIC, "TagA", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            System.out.println("SEND #" + i
                    + " status=" + result.getSendStatus()
                    + " queueId=" + result.getMessageQueue().getQueueId()
                    + " offset=" + result.getQueueOffset()
                    + " msgId=" + result.getMsgId()
                    + " body=" + body);
        }

        boolean firstBatchCompleted = firstBatchLatch.await(30, TimeUnit.SECONDS);
        System.out.println("FIRST_BATCH completed=" + firstBatchCompleted
                + " totalReceived=" + totalReceived.get()
                + " uniqueBodies=" + uniqueBodies.size());

        for (int i = 0; i < OBSERVE_SECONDS; i++) {
            Thread.sleep(1000L);
            if ((i + 1) % 10 == 0) {
                System.out.println("OBSERVE second=" + (i + 1)
                        + " totalReceived=" + totalReceived.get()
                        + " uniqueBodies=" + uniqueBodies.size());
            }
        }

        producer.shutdown();
        consumer.shutdown();

        int duplicateBodies = 0;
        for (AtomicInteger count : receiveCountByBody.values()) {
            if (count.get() > 1) {
                duplicateBodies++;
            }
        }

        System.out.println("RESULT totalReceived=" + totalReceived.get()
                + " uniqueBodies=" + uniqueBodies.size()
                + " expected=" + MESSAGE_COUNT
                + " duplicateBodies=" + duplicateBodies);

        if (!firstBatchCompleted) {
            throw new IllegalStateException("Initial messages were not fully consumed");
        }
        if (duplicateBodies > 0) {
            throw new IllegalStateException("Duplicate consumption observed");
        }
    }
}
