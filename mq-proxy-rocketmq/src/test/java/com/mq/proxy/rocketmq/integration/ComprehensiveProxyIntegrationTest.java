package com.mq.proxy.rocketmq.integration;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * 综合集成测试：通过 Proxy 收发消息，验证以下场景：
 * 1. 发送100条消息，验证收发一致性（不丢不重）
 * 2. 2个消费者集群模式负载均衡
 * 3. 广播模式：每个消费者都收到全部消息
 * 4. 顺序消息：同一队列消息顺序消费
 * 5. 消费重试：消息消费失败后重试最终成功
 * 6. Tag过滤：消费者只接收匹配Tag的消息
 * 7. 延迟消息：消息延迟投递
 */
public class ComprehensiveProxyIntegrationTest {

    private static final String NAMESRV_ADDR =
            System.getProperty("test.namesrvAddr", RocketMQIntegrationSupport.DEFAULT_NAMESRV_ADDR);
    private static final String TOPIC_PREFIX = "COMP_TEST_" + System.currentTimeMillis();
    private static final String PRODUCER_GROUP_PREFIX = "PID_COMP_TEST_" + System.currentTimeMillis();
    private static final int TOPIC_QUEUE_NUMS = 4;
    private EmbeddedRocketMQProxy proxy;

    // 用于创建Topic的直连Producer（不经过Proxy）
    private DefaultMQProducer topicCreator;

    @Before
    public void setUp() throws Exception {
        proxy = new EmbeddedRocketMQProxy(NAMESRV_ADDR);
        proxy.start();
        System.out.println("=== Proxy started on " + proxy.getProxyAddr() + ", broker=" + proxy.getBrokerAddr() + " ===");

        // 创建直连namesrv的Producer用于创建Topic
        topicCreator = new DefaultMQProducer("PID_TOPIC_CREATOR_" + System.currentTimeMillis());
        topicCreator.setNamesrvAddr(NAMESRV_ADDR);
        topicCreator.setInstanceName("TopicCreator");
        topicCreator.setSendMsgTimeout(10000);
        topicCreator.setRetryTimesWhenSendFailed(0);
        topicCreator.start();
    }

    @After
    public void tearDown() {
        if (topicCreator != null) {
            topicCreator.shutdown();
        }
        if (proxy != null) {
            proxy.shutdown();
        }
    }

    private void ensureTopicExists(String topic) throws Exception {
        try {
            topicCreator.createTopic("TBW102", topic, TOPIC_QUEUE_NUMS);
            System.out.println("[Setup] Topic created: " + topic + ", queues=" + TOPIC_QUEUE_NUMS);
        } catch (Exception e) {
            System.out.println("[Setup] Topic creation result for " + topic + ": " + e.getMessage());
        }
    }

    // ==================== 场景1: 发送100条消息，验证收发一致性 ====================

    @Test
    public void testSend100MessagesAllReceived() throws Exception {
        String topic = uniqueTopic("SEND100");
        String group = "CID_COMP_SEND100_" + System.currentTimeMillis();
        int totalMessages = 100;
        CountDownLatch consumeLatch = new CountDownLatch(totalMessages);
        Set<String> uniqueBodies = ConcurrentHashMap.newKeySet();
        AtomicInteger receivedCount = new AtomicInteger(0);

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer = createConsumer(group, "Send100Consumer");
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (uniqueBodies.add(body)) {
                            int count = receivedCount.incrementAndGet();
                            consumeLatch.countDown();
                            if (count % 20 == 0) {
                                System.out.println("[Send100] received " + count + " messages so far...");
                            }
                        } else {
                            System.out.println("[Send100] DUPLICATE detected: " + body);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_SEND100", "Send100Producer");

        try {
            consumer.start();
            Thread.sleep(5000L);
            producer.start();

            int sendSuccess = 0;
            for (int i = 0; i < totalMessages; i++) {
                String body = "msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TAG_SEND100", "KEY_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult result = producer.send(msg);
                if (result.getSendStatus() == org.apache.rocketmq.client.producer.SendStatus.SEND_OK) {
                    sendSuccess++;
                }
                if ((i + 1) % 20 == 0) {
                    System.out.println("[Send100] sent " + (i + 1) + " messages...");
                }
            }

            System.out.println("[Send100] sent " + sendSuccess + "/" + totalMessages + " messages successfully");

            boolean completed = consumeLatch.await(120, TimeUnit.SECONDS);
            assertTrue("Should receive all 100 messages within timeout, received=" + receivedCount.get(), completed);
            assertEquals("All 100 messages should be received exactly once (no duplicates)", totalMessages, uniqueBodies.size());
            assertEquals("Received count should equal sent count", totalMessages, receivedCount.get());

            System.out.println("[Send100] PASS: sent=" + sendSuccess + ", received=" + receivedCount.get()
                    + ", unique=" + uniqueBodies.size());
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    // ==================== 场景2: 2个消费者负载均衡 ====================

    @Test
    public void testTwoConsumersLoadBalanced() throws Exception {
        String topic = uniqueTopic("LB");
        String group = "CID_COMP_LB_" + System.currentTimeMillis();
        int totalMessages = 100;
        CountDownLatch consumeLatch = new CountDownLatch(totalMessages);
        Set<String> uniqueBodies = ConcurrentHashMap.newKeySet();
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer1 = createConsumer(group, "LBConsumer1");
        DefaultMQPushConsumer consumer2 = createConsumer(group, "LBConsumer2");
        consumer1.subscribe(topic, "*");
        consumer2.subscribe(topic, "*");
        consumer1.registerMessageListener(createListener("lb-consumer-1", uniqueBodies, consumer1Count, consumeLatch));
        consumer2.registerMessageListener(createListener("lb-consumer-2", uniqueBodies, consumer2Count, consumeLatch));

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_LB", "LBProducer");

        try {
            consumer1.start();
            consumer2.start();
            Thread.sleep(5000L);
            producer.start();

            for (int i = 0; i < totalMessages; i++) {
                String body = "lb-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TAG_LB", "KEY_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg);
            }

            boolean completed = consumeLatch.await(120, TimeUnit.SECONDS);
            assertTrue("All 100 messages should be consumed, received=" + (consumer1Count.get() + consumer2Count.get()), completed);
            assertEquals("No message should be consumed twice", totalMessages, uniqueBodies.size());

            int c1 = consumer1Count.get();
            int c2 = consumer2Count.get();
            System.out.println("[LoadBalance] consumer-1: " + c1 + ", consumer-2: " + c2);

            assertTrue("consumer-1 should receive messages, actual=" + c1, c1 > 0);
            assertTrue("consumer-2 should receive messages, actual=" + c2, c2 > 0);
            assertEquals("Total consumed should equal sent", totalMessages, c1 + c2);

            double ratio = (double) Math.min(c1, c2) / Math.max(c1, c2);
            System.out.println("[LoadBalance] balance ratio: " + String.format("%.2f", ratio));
            assertTrue("Load should be reasonably balanced, ratio=" + String.format("%.2f", ratio)
                    + ", c1=" + c1 + ", c2=" + c2, ratio >= 0.3);
        } finally {
            producer.shutdown();
            consumer1.shutdown();
            consumer2.shutdown();
        }
    }

    // ==================== 场景3: 广播模式 ====================

    @Test
    public void testBroadcastModeEachConsumerReceivesAll() throws Exception {
        String topic = uniqueTopic("BCAST");
        String group = "CID_COMP_BCAST_" + System.currentTimeMillis();
        int totalMessages = 50;
        CountDownLatch consumer1Latch = new CountDownLatch(totalMessages);
        CountDownLatch consumer2Latch = new CountDownLatch(totalMessages);
        Set<String> consumer1Bodies = ConcurrentHashMap.newKeySet();
        Set<String> consumer2Bodies = ConcurrentHashMap.newKeySet();
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer1 = createConsumer(group, "BCastConsumer1");
        DefaultMQPushConsumer consumer2 = createConsumer(group, "BCastConsumer2");
        consumer1.setMessageModel(MessageModel.BROADCASTING);
        consumer2.setMessageModel(MessageModel.BROADCASTING);
        consumer1.subscribe(topic, "*");
        consumer2.subscribe(topic, "*");
        consumer1.registerMessageListener(createListener("bcast-consumer-1", consumer1Bodies, consumer1Count, consumer1Latch));
        consumer2.registerMessageListener(createListener("bcast-consumer-2", consumer2Bodies, consumer2Count, consumer2Latch));

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_BCAST", "BCastProducer");

        try {
            producer.start();
            consumer1.start();
            consumer2.start();
            Thread.sleep(3000L);

            for (int i = 0; i < totalMessages; i++) {
                String body = "bcast-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TAG_BCAST", "KEY_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg);
            }

            boolean c1Done = consumer1Latch.await(120, TimeUnit.SECONDS);
            boolean c2Done = consumer2Latch.await(120, TimeUnit.SECONDS);

            assertTrue("Broadcast consumer-1 should receive all messages, got=" + consumer1Count.get(), c1Done);
            assertTrue("Broadcast consumer-2 should receive all messages, got=" + consumer2Count.get(), c2Done);
            assertEquals("Consumer-1 should receive exactly " + totalMessages + " messages",
                    totalMessages, consumer1Bodies.size());
            assertEquals("Consumer-2 should receive exactly " + totalMessages + " messages",
                    totalMessages, consumer2Bodies.size());

            System.out.println("[Broadcast] PASS: consumer-1=" + consumer1Count.get()
                    + ", consumer-2=" + consumer2Count.get());
        } finally {
            producer.shutdown();
            consumer1.shutdown();
            consumer2.shutdown();
        }
    }

    // ==================== 场景4: 顺序消息 ====================

    @Test
    public void testOrderlyMessagePreservesOrder() throws Exception {
        String topic = uniqueTopic("ORDER");
        String group = "CID_COMP_ORDER_" + System.currentTimeMillis();
        int totalMessages = 50;
        CountDownLatch consumeLatch = new CountDownLatch(totalMessages);
        List<Integer> consumedOrder = Collections.synchronizedList(new ArrayList<Integer>());

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer = createConsumer(group, "OrderConsumer");
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        int index = Integer.parseInt(body.substring(body.lastIndexOf('-') + 1));
                        consumedOrder.add(index);
                        consumeLatch.countDown();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_ORDER", "OrderProducer");
        MessageQueueSelector selector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                int index = ((Integer) arg) % mqs.size();
                return mqs.get(index);
            }
        };

        try {
            consumer.start();
            Thread.sleep(5000L);
            producer.start();

            for (int i = 0; i < totalMessages; i++) {
                String body = "order-step-" + i;
                Message msg = new Message(topic, "TAG_ORDER", "ORDER_KEY",
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, selector, 0);
            }

            boolean completed = consumeLatch.await(120, TimeUnit.SECONDS);
            assertTrue("All ordered messages should be consumed", completed);
            assertEquals(totalMessages, consumedOrder.size());

            for (int i = 0; i < totalMessages; i++) {
                assertEquals("Order mismatch at position " + i, i, consumedOrder.get(i).intValue());
            }

            System.out.println("[Orderly] PASS: all " + totalMessages + " messages consumed in order");
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    // ==================== 场景5: 消费重试 ====================

    @Test
    public void testConsumeRetryEventuallySucceeds() throws Exception {
        String topic = uniqueTopic("RETRY");
        String group = "CID_COMP_RETRY_" + System.currentTimeMillis();
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch successLatch = new CountDownLatch(1);
        CountDownLatch warmupLatch = new CountDownLatch(1);
        String warmupBody = "retry-warmup-" + UUID.randomUUID();
        String retryBody = "retry-body-" + UUID.randomUUID();

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer = createConsumer(group, "RetryConsumer");
        consumer.setMaxReconsumeTimes(3);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (warmupBody.equals(body)) {
                            warmupLatch.countDown();
                            continue;
                        }
                        if (retryBody.equals(body)) {
                            int currentAttempt = attempts.incrementAndGet();
                            System.out.println("[Retry] attempt=" + currentAttempt
                                    + ", reconsumeTimes=" + msg.getReconsumeTimes());
                            if (currentAttempt <= 3) {
                                context.setDelayLevelWhenNextConsume(1);
                                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                            }
                            successLatch.countDown();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_RETRY", "RetryProducer");

        try {
            producer.start();
            producer.send(new Message(topic, "TAG_RETRY", "WARMUP",
                    warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));

            consumer.start();
            assertTrue("Warmup message should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_RETRY", "RETRY_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());

            boolean success = successLatch.await(120, TimeUnit.SECONDS);
            assertTrue("Message should eventually succeed after retries", success);
            assertEquals("Message should be attempted 4 times (1 original + 3 retries)", 4, attempts.get());

            System.out.println("[Retry] PASS: message succeeded after " + attempts.get() + " attempts");
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    // ==================== 场景6: Tag过滤 ====================

    @Test
    public void testTagFiltering() throws Exception {
        String topic = uniqueTopic("TAG");
        String group = "CID_COMP_TAG_" + System.currentTimeMillis();
        int tagAMessages = 30;
        int tagBMessages = 20;
        CountDownLatch tagALatch = new CountDownLatch(tagAMessages);
        Set<String> receivedTagABodies = ConcurrentHashMap.newKeySet();
        AtomicInteger receivedOtherTag = new AtomicInteger(0);

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer = createConsumer(group, "TagConsumer");
        consumer.subscribe(topic, "TagA");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        String tag = msg.getTags();
                        System.out.println("[TagFilter] received: tag=" + tag + ", body=" + body);
                        if ("TagA".equals(tag)) {
                            receivedTagABodies.add(body);
                            tagALatch.countDown();
                        } else {
                            receivedOtherTag.incrementAndGet();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_TAG", "TagProducer");

        try {
            consumer.start();
            Thread.sleep(5000L);
            producer.start();

            for (int i = 0; i < tagAMessages; i++) {
                String body = "tagA-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TagA", "KEY_A_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg);
            }

            for (int i = 0; i < tagBMessages; i++) {
                String body = "tagB-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TagB", "KEY_B_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg);
            }

            boolean completed = tagALatch.await(120, TimeUnit.SECONDS);
            assertTrue("Should receive all TagA messages, got=" + receivedTagABodies.size(), completed);
            assertEquals("Should receive exactly " + tagAMessages + " TagA messages",
                    tagAMessages, receivedTagABodies.size());
            assertEquals("Should NOT receive TagB messages", 0, receivedOtherTag.get());

            System.out.println("[TagFilter] PASS: received " + receivedTagABodies.size()
                    + " TagA messages, 0 TagB messages");
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    // ==================== 场景7: 延迟消息 ====================

    @Test
    public void testDelayedMessage() throws Exception {
        String topic = uniqueTopic("DELAY");
        String group = "CID_COMP_DELAY_" + System.currentTimeMillis();
        CountDownLatch consumeLatch = new CountDownLatch(1);
        AtomicLong receiveTimestamp = new AtomicLong(0L);
        CountDownLatch warmupLatch = new CountDownLatch(1);
        String warmupBody = "delay-warmup-" + UUID.randomUUID();
        String delayedBody = "delayed-body-" + UUID.randomUUID();

        ensureTopicExists(topic);

        DefaultMQPushConsumer consumer = createConsumer(group, "DelayConsumer");
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (warmupBody.equals(body)) {
                            warmupLatch.countDown();
                            continue;
                        }
                        if (delayedBody.equals(body)) {
                            receiveTimestamp.compareAndSet(0L, System.currentTimeMillis());
                            consumeLatch.countDown();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_DELAY", "DelayProducer");

        try {
            producer.start();
            producer.send(new Message(topic, "TAG_DELAY", "WARMUP",
                    warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));

            consumer.start();
            assertTrue("Warmup message should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_DELAY", "DELAY_KEY",
                    delayedBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            msg.setDelayTimeLevel(1);
            long sendTimestamp = System.currentTimeMillis();
            SendResult result = producer.send(msg);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());

            assertFalse("Delayed message should not be consumed immediately",
                    consumeLatch.await(500, TimeUnit.MILLISECONDS));

            assertTrue("Delayed message should be consumed after delay",
                    consumeLatch.await(30, TimeUnit.SECONDS));

            long delay = receiveTimestamp.get() - sendTimestamp;
            assertTrue("Delayed message should arrive after at least 800ms, actual=" + delay + "ms", delay >= 800L);

            System.out.println("[Delay] PASS: message delayed " + delay + "ms before delivery");
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    // ==================== 辅助方法 ====================

    private DefaultMQPushConsumer createConsumer(String group, String instanceName) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(proxy.getProxyAddr());
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(4);
        return consumer;
    }

    private DefaultMQProducer createProducer(String producerGroup, String instanceName) {
        DefaultMQProducer producer = new DefaultMQProducer(producerGroup);
        producer.setNamesrvAddr(proxy.getProxyAddr());
        producer.setInstanceName(instanceName);
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);
        return producer;
    }

    private MessageListenerConcurrently createListener(String consumerName, Set<String> uniqueBodies,
                                                       AtomicInteger consumerCount, CountDownLatch consumeLatch) {
        return new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (uniqueBodies.add(body)) {
                            int count = consumerCount.incrementAndGet();
                            consumeLatch.countDown();
                            if (count % 10 == 0) {
                                System.out.println("[" + consumerName + "] received " + count + " messages so far...");
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        };
    }

    private String uniqueTopic(String scenario) {
        return TOPIC_PREFIX + "_" + scenario;
    }
}
