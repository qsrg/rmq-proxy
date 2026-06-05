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

public class IsolatedNativeClientTest {

    private static final String PROXY_ADDR = System.getProperty("proxy.addr", "127.0.0.1:10913");
    private static final String TOPIC = System.getProperty("test.topic", "IsolatedNativeTopic");
    private static final int MESSAGE_COUNT = Integer.getInteger("test.messageCount", 5);

    public static void main(String[] args) throws Exception {
        String runId = Long.toHexString(System.currentTimeMillis());
        String producerGroup = "IsoProducerGroup_" + runId;
        String consumerGroup = "IsoConsumerGroup_" + runId;

        System.out.println("=== Isolated Native Client Test ===");
        System.out.println("Proxy: " + PROXY_ADDR);
        System.out.println("Topic: " + TOPIC);
        System.out.println("ProducerGroup: " + producerGroup);
        System.out.println("ConsumerGroup: " + consumerGroup);

        CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);
        AtomicInteger receivedCount = new AtomicInteger(0);
        Set<String> uniqueBodies = ConcurrentHashMap.newKeySet();

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
                        uniqueBodies.add(body);
                        int current = receivedCount.incrementAndGet();
                        System.out.println("RECV #" + current + " msgId=" + msg.getMsgId()
                                + " queueId=" + msg.getQueueId()
                                + " offset=" + msg.getQueueOffset()
                                + " body=" + body);
                        latch.countDown();
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
            String body = "isolated-" + runId + "-" + i + "-" + UUID.randomUUID();
            Message msg = new Message(TOPIC, "TagA", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            System.out.println("SEND #" + i + " status=" + result.getSendStatus()
                    + " broker=" + result.getMessageQueue().getBrokerName()
                    + " queueId=" + result.getMessageQueue().getQueueId()
                    + " offset=" + result.getQueueOffset()
                    + " msgId=" + result.getMsgId());
        }

        boolean completed = latch.await(20, TimeUnit.SECONDS);

        producer.shutdown();
        consumer.shutdown();

        System.out.println("RESULT completed=" + completed
                + " received=" + receivedCount.get()
                + " uniqueBodies=" + uniqueBodies.size()
                + " expected=" + MESSAGE_COUNT);

        if (!completed || uniqueBodies.size() != MESSAGE_COUNT) {
            throw new IllegalStateException("Isolated native client test failed");
        }
    }
}
