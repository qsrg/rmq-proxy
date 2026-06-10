package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
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

public class NativeClientIntegrationTest {

    private static final String NAMESRV_ADDR =
            System.getProperty("test.namesrvAddr", RocketMQIntegrationSupport.DEFAULT_NAMESRV_ADDR);
    private static final String TOPIC_PREFIX = "NATIVE_CLIENT_TEST_" + System.currentTimeMillis();
    private static final String PRODUCER_GROUP_PREFIX = "PID_NATIVE_TEST_" + System.currentTimeMillis();
    private EmbeddedRocketMQProxy proxy;

    @Before
    public void setUp() throws Exception {
        proxy = new EmbeddedRocketMQProxy(NAMESRV_ADDR);
        proxy.start();
        System.out.println("Proxy started on " + proxy.getProxyAddr() + ", broker=" + proxy.getBrokerAddr());
    }

    @After
    public void tearDown() {
        if (proxy != null) {
            proxy.shutdown();
        }
    }

    @Test
    public void testProxyGetRouteInfo() throws Exception {
        String topic = uniqueTopic("ROUTE");
        String proxyNamesrvAddr = proxy.getProxyAddr();

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_PREFIX + "_ROUTE");
        producer.setNamesrvAddr(proxyNamesrvAddr);
        producer.setInstanceName("NativeRouteProducerTest");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        NettyRemotingClient testClient = new NettyRemotingClient(new NettyClientConfig());
        testClient.start();

        try {
            producer.start();
            Message msg = new Message(topic, "TAG_ROUTE", "KEY_ROUTE",
                    "warm up route".getBytes("UTF-8"));
            producer.send(msg);

            com.mq.proxy.core.protocol.RemotingCommand request =
                com.mq.proxy.core.protocol.RemotingCommand.createRequestCommand(
                    com.mq.proxy.core.protocol.RequestCode.GET_ROUTEINFO_BY_TOPIC,
                    new com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader(topic));
            request.makeCustomHeaderToNet();

            System.out.println("Sending route request to proxy at " + proxyNamesrvAddr);
            com.mq.proxy.core.protocol.RemotingCommand response =
                testClient.invokeSync(proxyNamesrvAddr, request, 5000);

            System.out.println("Route response code: " + response.getCode());
            System.out.println("Route response body: " + (response.getBody() != null ?
                new String(response.getBody(), java.nio.charset.StandardCharsets.UTF_8) : "null"));

            assertEquals("Route response should be SUCCESS",
                com.mq.proxy.core.protocol.RemotingSysResponseCode.SUCCESS, response.getCode());
            assertNotNull("Route response body should not be null", response.getBody());
        } finally {
            producer.shutdown();
            testClient.shutdown();
        }
    }

    @Test
    public void testNativeProducerSendMessage() throws Exception {
        String topic = uniqueTopic("SEND");
        String proxyNamesrvAddr = proxy.getProxyAddr();

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_PREFIX + "_SEND");
        producer.setNamesrvAddr(proxyNamesrvAddr);
        producer.setInstanceName("NativeProducerTest");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        try {
            producer.start();
            System.out.println("Native producer started, sending message...");

            Message msg = new Message(topic, "TAG_NATIVE", "KEY_NATIVE",
                    "Hello from native RocketMQ client!".getBytes("UTF-8"));

            SendResult sendResult = producer.send(msg);
            System.out.println("Send result: " + sendResult);

            assertNotNull("SendResult should not be null", sendResult);
            assertEquals("Send status should be SEND_OK",
                    org.apache.rocketmq.client.producer.SendStatus.SEND_OK,
                    sendResult.getSendStatus());
            assertNotNull("MsgId should not be null", sendResult.getMsgId());
        } finally {
            producer.shutdown();
        }
    }

    @Test
    public void testMultipleNativeClientsProduceAndConsumersLoadBalanceInClusterMode() throws Exception {
        String topic = uniqueTopic("CLUSTER");
        String group = "CID_NATIVE_CLUSTER_" + System.currentTimeMillis();
        int totalMessages = 16;
        CountDownLatch consumeLatch = new CountDownLatch(totalMessages);
        Set<String> uniqueBodies = ConcurrentHashMap.newKeySet();
        AtomicInteger consumer1Count = new AtomicInteger(0);
        AtomicInteger consumer2Count = new AtomicInteger(0);

        DefaultMQPushConsumer consumer1 = createConsumer(group, "NativeClusterConsumer1");
        DefaultMQPushConsumer consumer2 = createConsumer(group, "NativeClusterConsumer2");

        consumer1.subscribe(topic, "*");
        consumer2.subscribe(topic, "*");
        consumer1.registerMessageListener(createListener("consumer-1", uniqueBodies, consumer1Count, consumeLatch));
        consumer2.registerMessageListener(createListener("consumer-2", uniqueBodies, consumer2Count, consumeLatch));

        DefaultMQProducer producer1 = createProducer(PRODUCER_GROUP_PREFIX + "_CLUSTER_1", "NativeClusterProducer1");
        DefaultMQProducer producer2 = createProducer(PRODUCER_GROUP_PREFIX + "_CLUSTER_2", "NativeClusterProducer2");

        try {
            consumer1.start();
            consumer2.start();
            Thread.sleep(5000L);

            producer1.start();
            producer2.start();
            for (int i = 0; i < totalMessages / 2; i++) {
                String body = "cluster-p1-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TAG_CLUSTER", "KEY_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult result = producer1.send(msg);
                assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());
            }
            for (int i = totalMessages / 2; i < totalMessages; i++) {
                String body = "cluster-p2-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TAG_CLUSTER", "KEY_" + i,
                        body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult result = producer2.send(msg);
                assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());
            }

            boolean completed = consumeLatch.await(60, TimeUnit.SECONDS);
            assertTrue("all cluster messages should be consumed", completed);
            assertEquals("all cluster messages should be consumed exactly once", totalMessages, uniqueBodies.size());
            assertTrue("consumer-1 should receive part of the traffic, actual=" + consumer1Count.get(),
                    consumer1Count.get() > 0);
            assertTrue("consumer-2 should receive part of the traffic, actual=" + consumer2Count.get(),
                    consumer2Count.get() > 0);
        } finally {
            producer1.shutdown();
            producer2.shutdown();
            consumer1.shutdown();
            consumer2.shutdown();
        }
    }

    @Test
    public void testNativeOrderedMessagesPreserveOrder() throws Exception {
        String topic = uniqueTopic("ORDER");
        String group = "CID_NATIVE_ORDER_" + System.currentTimeMillis();
        int totalMessages = 8;
        CountDownLatch consumeLatch = new CountDownLatch(totalMessages);
        List<Integer> consumedOrder = Collections.synchronizedList(new ArrayList<Integer>());

        DefaultMQPushConsumer consumer = createConsumer(group, "NativeOrderConsumer");
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        consumedOrder.add(Integer.parseInt(body.substring(body.lastIndexOf('-') + 1)));
                        consumeLatch.countDown();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_ORDER", "NativeOrderProducer");
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
                Message msg = new Message(topic, "TAG_ORDER", "ORDER_KEY", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult result = producer.send(msg, selector, Integer.valueOf(0));
                assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());
            }

            assertTrue("ordered messages should all be consumed", consumeLatch.await(60, TimeUnit.SECONDS));
            assertEquals(totalMessages, consumedOrder.size());
            for (int i = 0; i < totalMessages; i++) {
                assertEquals("ordered message index mismatch at position " + i, i, consumedOrder.get(i).intValue());
            }
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    @Test
    public void testNativeOrderedConsumeRetriesLocallyAndEventuallySucceeds() throws Exception {
        String topic = uniqueTopic("ORDER_RETRY");
        String group = "CID_NATIVE_ORDER_RETRY_" + System.currentTimeMillis();
        String warmupBody = "order-retry-warmup-" + UUID.randomUUID();
        String retryBody = "order-retry-body-" + UUID.randomUUID();
        CountDownLatch warmupLatch = new CountDownLatch(1);
        CountDownLatch successLatch = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicInteger retryTopicAttempts = new AtomicInteger(0);
        AtomicLong firstAttemptAt = new AtomicLong(0L);
        AtomicLong successAt = new AtomicLong(0L);
        List<String> messageTopics = Collections.synchronizedList(new ArrayList<String>());
        List<String> queueTopics = Collections.synchronizedList(new ArrayList<String>());
        List<String> retryTopics = Collections.synchronizedList(new ArrayList<String>());
        List<String> attemptTrace = Collections.synchronizedList(new ArrayList<String>());

        DefaultMQPushConsumer consumer = createConsumer(group, "NativeOrderRetryConsumer");
        consumer.setMaxReconsumeTimes(2);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (warmupBody.equals(body)) {
                            warmupLatch.countDown();
                            continue;
                        }
                        if (!retryBody.equals(body)) {
                            continue;
                        }

                        long now = System.currentTimeMillis();
                        firstAttemptAt.compareAndSet(0L, now);
                        int currentAttempt = attempts.incrementAndGet();
                        String currentTopic = msg.getTopic();
                        String currentQueueTopic = context.getMessageQueue().getTopic();
                        String retrySourceTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        String brokerName = context.getMessageQueue().getBrokerName();
                        messageTopics.add(currentTopic);
                        queueTopics.add(currentQueueTopic);
                        retryTopics.add(retrySourceTopic);
                        attemptTrace.add("attempt=" + currentAttempt
                                + ", messageTopic=" + currentTopic
                                + ", queueTopic=" + currentQueueTopic
                                + ", retrySourceTopic=" + retrySourceTopic
                                + ", broker=" + brokerName
                                + ", queueId=" + context.getMessageQueue().getQueueId()
                                + ", reconsumeTimes=" + msg.getReconsumeTimes());
                        System.out.println("order-retry " + attemptTrace.get(attemptTrace.size() - 1));

                        if (currentAttempt <= 2) {
                            context.setSuspendCurrentQueueTimeMillis(1000);
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                        successAt.compareAndSet(0L, now);
                        successLatch.countDown();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_ORDER_RETRY", "NativeOrderRetryProducer");
        MessageQueueSelector selector = new MessageQueueSelector() {
            @Override
            public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                int queueId = (Integer) arg;
                for (MessageQueue mq : mqs) {
                    if (mq.getQueueId() == queueId) {
                        return mq;
                    }
                }
                return mqs.get(0);
            }
        };

        try {
            consumer.start();
            Thread.sleep(5000L);
            producer.start();

            SendResult warmupResult = producer.send(
                    new Message(topic, "TAG_ORDER_RETRY", "WARMUP", warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)),
                    selector, Integer.valueOf(0));
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, warmupResult.getSendStatus());
            assertTrue("ordered retry warmup message should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_ORDER_RETRY", "ORDER_RETRY_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg, selector, Integer.valueOf(warmupResult.getMessageQueue().getQueueId()));
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());

            assertTrue("ordered retry message should eventually succeed within local retry window, trace=" + attemptTrace,
                    successLatch.await(30, TimeUnit.SECONDS));
            assertEquals("ordered retry message should be consumed twice on origin topic and then succeed locally, trace="
                    + attemptTrace, 3, attempts.get());
            assertEquals("ordered retry should not enter retry topic before success, trace=" + attemptTrace,
                    0, retryTopicAttempts.get());
            assertEquals("listener should always receive business topic, trace=" + attemptTrace,
                    topic, messageTopics.get(0));
            assertEquals("listener should always receive business topic, trace=" + attemptTrace,
                    topic, messageTopics.get(1));
            assertEquals("listener should always receive business topic, trace=" + attemptTrace,
                    topic, messageTopics.get(2));
            assertEquals("attempt 1 should consume from origin queue, trace=" + attemptTrace, topic, queueTopics.get(0));
            assertEquals("attempt 2 should still consume from origin queue, trace=" + attemptTrace, topic, queueTopics.get(1));
            assertEquals("attempt 3 should still consume from origin queue, trace=" + attemptTrace, topic, queueTopics.get(2));
            assertNull("origin attempts should not carry retry source topic, trace=" + attemptTrace, retryTopics.get(0));
            assertNull("origin attempts should not carry retry source topic, trace=" + attemptTrace, retryTopics.get(1));
            assertNull("local retry should not carry retry source topic, trace=" + attemptTrace, retryTopics.get(2));
            assertTrue("ordered local retry should finish promptly, elapsed="
                            + (successAt.get() - firstAttemptAt.get()) + "ms, trace=" + attemptTrace,
                    successAt.get() - firstAttemptAt.get() < 15000L);
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    @Test
    public void testNativeDelayedMessageDeliveredLater() throws Exception {
        String topic = uniqueTopic("DELAY");
        String group = "CID_NATIVE_DELAY_" + System.currentTimeMillis();
        CountDownLatch consumeLatch = new CountDownLatch(1);
        AtomicLong receiveTimestamp = new AtomicLong(0L);
        CountDownLatch warmupLatch = new CountDownLatch(1);
        String warmupBody = "delay-warmup-" + UUID.randomUUID();
        String delayedBody = "delayed-body-" + UUID.randomUUID();

        DefaultMQPushConsumer consumer = createConsumer(group, "NativeDelayConsumer");
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (warmupBody.equals(body)) {
                            warmupLatch.countDown();
                        } else if (delayedBody.equals(body)) {
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

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_DELAY", "NativeDelayProducer");

        try {
            producer.start();
            producer.send(new Message(topic, "TAG_DELAY", "WARMUP", warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));

            consumer.start();
            assertTrue("warmup message should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_DELAY", "DELAY_KEY", delayedBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            msg.setDelayTimeLevel(1);
            long sendTimestamp = System.currentTimeMillis();
            SendResult result = producer.send(msg);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());

            assertFalse("delayed message should not be consumed immediately", consumeLatch.await(500, TimeUnit.MILLISECONDS));
            assertTrue("delayed message should be consumed later", consumeLatch.await(30, TimeUnit.SECONDS));
            assertTrue("delayed message should arrive after at least 800ms, actual="
                    + (receiveTimestamp.get() - sendTimestamp),
                    receiveTimestamp.get() - sendTimestamp >= 800L);
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    @Test
    public void testNativeBroadcastConsumersEachReceiveAllMessages() throws Exception {
        String topic = uniqueTopic("BROADCAST");
        String group = "CID_NATIVE_BROADCAST_" + System.currentTimeMillis();
        int totalMessages = 6;
        CountDownLatch consumer1Latch = new CountDownLatch(totalMessages);
        CountDownLatch consumer2Latch = new CountDownLatch(totalMessages);
        CountDownLatch consumer1WarmupLatch = new CountDownLatch(1);
        CountDownLatch consumer2WarmupLatch = new CountDownLatch(1);
        Set<String> consumer1Bodies = ConcurrentHashMap.newKeySet();
        Set<String> consumer2Bodies = ConcurrentHashMap.newKeySet();
        String warmupBody = "broadcast-warmup-" + UUID.randomUUID();

        DefaultMQPushConsumer consumer1 = createConsumer(group, "NativeBroadcastConsumer1");
        DefaultMQPushConsumer consumer2 = createConsumer(group, "NativeBroadcastConsumer2");
        consumer1.setMessageModel(MessageModel.BROADCASTING);
        consumer2.setMessageModel(MessageModel.BROADCASTING);
        consumer1.subscribe(topic, "*");
        consumer2.subscribe(topic, "*");
        consumer1.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    if (warmupBody.equals(body)) {
                        consumer1WarmupLatch.countDown();
                        continue;
                    }
                    consumer1Bodies.add(body);
                    System.out.println("broadcast-consumer-1 received msgId=" + msg.getMsgId()
                            + ", queueId=" + msg.getQueueId() + ", body=" + body);
                    consumer1Latch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer2.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                try {
                    String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                    if (warmupBody.equals(body)) {
                        consumer2WarmupLatch.countDown();
                        continue;
                    }
                    consumer2Bodies.add(body);
                    System.out.println("broadcast-consumer-2 received msgId=" + msg.getMsgId()
                            + ", queueId=" + msg.getQueueId() + ", body=" + body);
                    consumer2Latch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_BROADCAST", "NativeBroadcastProducer");

        try {
            producer.start();
            producer.createTopic("TBW102", topic, 4);
            consumer1.start();
            consumer2.start();

            Message warmup = new Message(topic, "TAG_BROADCAST", "WARMUP",
                    warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult warmupResult = producer.send(warmup);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, warmupResult.getSendStatus());

            assertTrue("broadcast consumer-1 should receive warmup message",
                    consumer1WarmupLatch.await(60, TimeUnit.SECONDS));
            assertTrue("broadcast consumer-2 should receive warmup message",
                    consumer2WarmupLatch.await(60, TimeUnit.SECONDS));

            for (int i = 0; i < totalMessages; i++) {
                String body = "broadcast-msg-" + i + "-" + UUID.randomUUID();
                Message msg = new Message(topic, "TAG_BROADCAST", "KEY_" + i, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult result = producer.send(msg);
                assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());
            }

            assertTrue("broadcast consumer-1 should receive all messages", consumer1Latch.await(60, TimeUnit.SECONDS));
            assertTrue("broadcast consumer-2 should receive all messages", consumer2Latch.await(60, TimeUnit.SECONDS));
            assertEquals(totalMessages, consumer1Bodies.size());
            assertEquals(totalMessages, consumer2Bodies.size());
            assertEquals(consumer1Bodies, consumer2Bodies);
        } finally {
            producer.shutdown();
            consumer1.shutdown();
            consumer2.shutdown();
        }
    }

    @Test
    public void testNativeConsumeRetryUpToFiveTimesThenSuccess() throws Exception {
        String topic = uniqueTopic("RETRY");
        String group = "CID_NATIVE_RETRY_" + System.currentTimeMillis();
        AtomicInteger attempts = new AtomicInteger(0);
        CountDownLatch successLatch = new CountDownLatch(1);
        CountDownLatch warmupLatch = new CountDownLatch(1);
        String warmupBody = "retry-warmup-" + UUID.randomUUID();
        String retryBody = "retry-body-" + UUID.randomUUID();

        DefaultMQPushConsumer consumer = createConsumer(group, "NativeRetryConsumer");
        consumer.setMaxReconsumeTimes(5);
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
                            if (currentAttempt <= 5) {
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

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_RETRY", "NativeRetryProducer");

        try {
            producer.start();
            SendResult warmupResult = producer.send(
                    new Message(topic, "TAG_RETRY", "WARMUP", warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));

            consumer.start();
            assertTrue("warmup retry message should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_RETRY", "RETRY_KEY", retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            int targetQueueId = warmupResult.getMessageQueue().getQueueId();
            SendResult result = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message message, Object arg) {
                    int queueId = (Integer) arg;
                    for (MessageQueue mq : mqs) {
                        if (mq.getQueueId() == queueId) {
                            return mq;
                        }
                    }
                    return mqs.get(0);
                }
            }, targetQueueId);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());

            assertTrue("message should succeed after retries", successLatch.await(120, TimeUnit.SECONDS));
            assertEquals("message should be consumed 6 times in total", 6, attempts.get());
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    private DefaultMQPushConsumer createConsumer(String group, String instanceName) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(proxy.getProxyAddr());
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
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
        return createListener(consumerName, uniqueBodies, consumerCount, consumeLatch, null);
    }

    private MessageListenerConcurrently createListener(String consumerName, Set<String> uniqueBodies,
                                                       AtomicInteger consumerCount, CountDownLatch consumeLatch,
                                                       AtomicLong receiveTimestamp) {
        return new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        if (uniqueBodies.add(body)) {
                            consumerCount.incrementAndGet();
                            if (receiveTimestamp != null) {
                                receiveTimestamp.compareAndSet(0L, System.currentTimeMillis());
                            }
                            consumeLatch.countDown();
                        }
                        System.out.println(consumerName + " received msgId=" + msg.getMsgId()
                                + ", queueId=" + msg.getQueueId()
                                + ", body=" + body);
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
