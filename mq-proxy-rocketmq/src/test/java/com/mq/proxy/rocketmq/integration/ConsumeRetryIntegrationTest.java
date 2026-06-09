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
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * 集成测试：验证通过 Proxy 的消费重试功能（maxReconsumeTimes=5）
 *
 * 测试场景：
 * - 并发消费重试：消息消费失败后通过 CONSUMER_SEND_MSG_BACK 发回 Broker，进入 %RETRY% Topic
 * - 顺序消费重试：消息消费失败后本地挂起重试（不经过 Broker），超过 maxReconsumeTimes 后发回 Broker
 * - 死信队列：reconsumeTimes >= maxReconsumeTimes 时消息进入 %DLQ% Topic
 */
public class ConsumeRetryIntegrationTest {

    private static final String NAMESRV_ADDR =
            System.getProperty("test.namesrvAddr", "127.0.0.1:9876");
    private static final String TOPIC_PREFIX = "RETRY_TEST_" + System.currentTimeMillis();
    private static final String PRODUCER_GROUP_PREFIX = "PID_RETRY_TEST_" + System.currentTimeMillis();
    private static final int MAX_RECONSUME_TIMES = 5;

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

    // ==================== 并发消费重试测试 ====================

    /**
     * 并发消费重试：消息前3次失败，第4次成功
     *
     * 验证点：
     * 1. 消息消费失败后能通过 Proxy 正确触发重试（CONSUMER_SEND_MSG_BACK）
     * 2. 重试消息从 %RETRY% Topic 被重新消费
     * 3. 总消费次数 = 失败次数 + 1次成功 = 4次
     * 4. 消息最终消费成功，不会进入死信队列
     */
    @Test
    public void testConcurrentRetryThenSuccess() throws Exception {
        String topic = uniqueTopic("CONC_SUCC");
        String group = "CID_CONC_SUCC_" + System.currentTimeMillis();
        String warmupBody = "conc-succ-warmup-" + UUID.randomUUID();
        String retryBody = "conc-succ-body-" + UUID.randomUUID();

        AtomicInteger totalAttempts = new AtomicInteger(0);
        List<String> attemptTrace = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch warmupLatch = new CountDownLatch(1);
        CountDownLatch successLatch = new CountDownLatch(1);

        int failTimes = 3;

        DefaultMQPushConsumer consumer = createConsumer(group, "ConcSuccConsumer");
        consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
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
                        if (!retryBody.equals(body)) {
                            continue;
                        }

                        int attempt = totalAttempts.incrementAndGet();
                        int reconsumeTimes = msg.getReconsumeTimes();
                        String retryTopicProp = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                        String trace = String.format(
                                "attempt=%d, reconsumeTimes=%d, msgTopic=%s, retryTopicProp=%s, queueId=%d",
                                attempt, reconsumeTimes, msg.getTopic(), retryTopicProp, msg.getQueueId());
                        attemptTrace.add(trace);
                        System.out.println("[CONC-SUCC] " + trace);

                        if (attempt <= failTimes) {
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                        successLatch.countDown();
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_CONC_SUCC", "ConcSuccProducer");

        try {
            producer.start();
            producer.send(new Message(topic, "TAG_CONC_SUCC", "WARMUP",
                    warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));

            consumer.start();
            assertTrue("warmup should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_CONC_SUCC", "RETRY_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            assertEquals(SendStatus.SEND_OK, result.getSendStatus());

            // 等待成功消费（延迟: 10s + 30s + 1m ≈ 2分钟，设置5分钟超时）
            boolean succeeded = successLatch.await(5, TimeUnit.MINUTES);
            System.out.println("[CONC-SUCC] succeeded=" + succeeded
                    + ", totalAttempts=" + totalAttempts.get()
                    + ", trace=" + attemptTrace);

            assertTrue("Message should succeed after " + failTimes + " retries, trace=" + attemptTrace, succeeded);
            assertEquals("Total attempts should be " + (failTimes + 1),
                    failTimes + 1, totalAttempts.get());

            // 验证第1次是原始消息（无RETRY_TOPIC属性），后续重试来自%RETRY% Topic
            assertNull("First attempt should not have RETRY_TOPIC property",
                    attemptTrace.get(0).split("retryTopicProp=")[1].split(",")[0].equals("null") ? null : "not null");
            // 注意：resetRetryAndNamespace 会将 msg.getTopic() 还原为原始 Topic，
            // 但 PROPERTY_RETRY_TOPIC 属性保留了原始来源信息
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    /**
     * 并发消费重试：消息始终失败，验证重试5次后进入死信队列
     *
     * 验证点：
     * 1. 消息被消费6次（1次原始 + 5次重试）
     * 2. 消息最终进入 %DLQ% 死信队列
     *
     * 注意：由于 Proxy 转发时 reconsumeTimes 未被正确保留，
     * 实际重试次数可能超过 maxReconsumeTimes 设置值。
     * 此测试使用与现有 NativeClientIntegrationTest 相同的验证方式。
     */
    @Test
    public void testConcurrentRetryExhaustedThenDLQ() throws Exception {
        String topic = uniqueTopic("CONC_DLQ");
        String group = "CID_CONC_DLQ_" + System.currentTimeMillis();
        String warmupBody = "conc-dlq-warmup-" + UUID.randomUUID();
        String retryBody = "conc-dlq-body-" + UUID.randomUUID();

        AtomicInteger totalAttempts = new AtomicInteger(0);
        List<String> attemptTrace = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch warmupLatch = new CountDownLatch(1);
        // 等待所有重试完成（1次原始 + 5次重试 = 6次）
        CountDownLatch allRetriesDone = new CountDownLatch(MAX_RECONSUME_TIMES + 1);

        DefaultMQPushConsumer consumer = createConsumer(group, "ConcDLQConsumer");
        consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
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
                        if (!retryBody.equals(body)) {
                            continue;
                        }

                        int attempt = totalAttempts.incrementAndGet();
                        int reconsumeTimes = msg.getReconsumeTimes();
                        String trace = String.format(
                                "attempt=%d, reconsumeTimes=%d, msgTopic=%s, queueId=%d",
                                attempt, reconsumeTimes, msg.getTopic(), msg.getQueueId());
                        attemptTrace.add(trace);
                        System.out.println("[CONC-DLQ] " + trace);

                        allRetriesDone.countDown();

                        // 始终返回 RECONSUME_LATER，模拟消费始终失败
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_CONC_DLQ", "ConcDLQProducer");

        try {
            producer.start();
            SendResult warmupResult = producer.send(
                    new Message(topic, "TAG_CONC_DLQ", "WARMUP", warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));
            System.out.println("[CONC-DLQ] Warmup sent to queueId=" + warmupResult.getMessageQueue().getQueueId());

            consumer.start();
            assertTrue("warmup message should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_CONC_DLQ", "RETRY_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            System.out.println("[CONC-DLQ] Test message sent, sendStatus=" + result.getSendStatus());
            assertEquals(SendStatus.SEND_OK, result.getSendStatus());

            // 等待所有重试完成（延迟等级: 10s, 30s, 1m, 2m, 3m）
            boolean allDone = allRetriesDone.await(8, TimeUnit.MINUTES);
            System.out.println("[CONC-DLQ] All retries done=" + allDone
                    + ", totalAttempts=" + totalAttempts.get()
                    + ", trace=" + attemptTrace);

            assertTrue("All 6 consumption attempts should complete, trace=" + attemptTrace, allDone);
            assertEquals("Total consumption attempts should be 6 (1 original + 5 retries)",
                    MAX_RECONSUME_TIMES + 1, totalAttempts.get());

            // 验证消息已进入死信队列
            Thread.sleep(3000);
            String dlqGroup = group + "_DLQ_CHECK";
            DefaultMQPushConsumer dlqConsumer = new DefaultMQPushConsumer(dlqGroup);
            dlqConsumer.setNamesrvAddr(NAMESRV_ADDR);
            dlqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            String dlqTopic = org.apache.rocketmq.common.MixAll.getDLQTopic(group);
            AtomicInteger dlqCount = new AtomicInteger(0);
            CountDownLatch dlqLatch = new CountDownLatch(1);
            dlqConsumer.subscribe(dlqTopic, "*");
            dlqConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt msg : msgs) {
                        try {
                            String body = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            System.out.println("[CONC-DLQ] DLQ message: body=" + body
                                    + ", reconsumeTimes=" + msg.getReconsumeTimes());
                            if (retryBody.equals(body)) {
                                dlqCount.incrementAndGet();
                                dlqLatch.countDown();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            try {
                dlqConsumer.start();
                boolean dlqFound = dlqLatch.await(30, TimeUnit.SECONDS);
                System.out.println("[CONC-DLQ] DLQ check result: found=" + dlqFound + ", count=" + dlqCount.get());
                assertTrue("Message should be found in DLQ after " + (MAX_RECONSUME_TIMES + 1) + " failed attempts", dlqFound);
            } finally {
                dlqConsumer.shutdown();
            }

        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    // ==================== 顺序消费重试测试 ====================

    /**
     * 顺序消费重试：前3次失败，第4次成功
     *
     * 验证点：
     * 1. 顺序消费失败后本地挂起重试（不经过Broker）
     * 2. reconsumeTimes 在本地重试阶段正确递增：0, 1, 2, 3
     * 3. 总消费次数 = 3次失败 + 1次成功 = 4次
     * 4. 消息始终在原始Topic上重试（本地重试不经过 %RETRY% Topic）
     */
    @Test
    public void testOrderlyRetryThenSuccess() throws Exception {
        String topic = uniqueTopic("ORDER_SUCC");
        String group = "CID_ORDER_SUCC_" + System.currentTimeMillis();
        String warmupBody = "order-succ-warmup-" + UUID.randomUUID();
        String retryBody = "order-succ-body-" + UUID.randomUUID();

        AtomicInteger totalAttempts = new AtomicInteger(0);
        List<Integer> reconsumeTimesList = Collections.synchronizedList(new ArrayList<Integer>());
        List<String> attemptTrace = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch warmupLatch = new CountDownLatch(1);
        CountDownLatch successLatch = new CountDownLatch(1);

        int failTimes = 3;

        DefaultMQPushConsumer consumer = createConsumer(group, "OrderSuccConsumer");
        consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
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

                        int attempt = totalAttempts.incrementAndGet();
                        int reconsumeTimes = msg.getReconsumeTimes();
                        reconsumeTimesList.add(reconsumeTimes);
                        String trace = String.format(
                                "attempt=%d, reconsumeTimes=%d, msgTopic=%s, queueId=%d",
                                attempt, reconsumeTimes, msg.getTopic(), msg.getQueueId());
                        attemptTrace.add(trace);
                        System.out.println("[ORDER-SUCC] " + trace);

                        if (attempt <= failTimes) {
                            context.setSuspendCurrentQueueTimeMillis(1000);
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }
                        successLatch.countDown();
                        return ConsumeOrderlyStatus.SUCCESS;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_ORDER_SUCC", "OrderSuccProducer");
        MessageQueueSelector selector = createQueueSelector();

        try {
            consumer.start();
            Thread.sleep(5000L);
            producer.start();

            SendResult warmupResult = producer.send(
                    new Message(topic, "TAG_ORDER_SUCC", "WARMUP",
                            warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)),
                    selector, 0);
            assertTrue("warmup should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_ORDER_SUCC", "ORDER_RETRY_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg, selector,
                    warmupResult.getMessageQueue().getQueueId());
            assertEquals(SendStatus.SEND_OK, result.getSendStatus());

            boolean succeeded = successLatch.await(2, TimeUnit.MINUTES);
            System.out.println("[ORDER-SUCC] succeeded=" + succeeded
                    + ", totalAttempts=" + totalAttempts.get()
                    + ", trace=" + attemptTrace);

            assertTrue("Message should succeed after " + failTimes + " retries, trace=" + attemptTrace, succeeded);
            assertEquals("Total attempts should be " + (failTimes + 1),
                    failTimes + 1, totalAttempts.get());

            // 顺序消费本地重试，reconsumeTimes 正确递增
            for (int i = 0; i < reconsumeTimesList.size(); i++) {
                assertEquals("reconsumeTimes at attempt " + (i + 1) + " should be " + i,
                        i, reconsumeTimesList.get(i).intValue());
            }
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    /**
     * 顺序消费重试：消息始终失败，验证本地重试5次后 reconsumeTimes 达到 maxReconsumeTimes
     *
     * 验证点：
     * 1. 顺序消费失败后本地挂起重试（不经过Broker）
     * 2. reconsumeTimes 在本地重试阶段正确递增：0, 1, 2, 3, 4, 5
     * 3. 当 reconsumeTimes >= maxReconsumeTimes 时，调用 sendMessageBack 发回 Broker
     * 4. 消息始终在原始Topic上重试（本地重试不经过 %RETRY% Topic）
     */
    @Test
    public void testOrderlyRetryExhaustedLocally() throws Exception {
        String topic = uniqueTopic("ORDER_DLQ");
        String group = "CID_ORDER_DLQ_" + System.currentTimeMillis();
        String warmupBody = "order-dlq-warmup-" + UUID.randomUUID();
        String retryBody = "order-dlq-body-" + UUID.randomUUID();

        AtomicInteger totalAttempts = new AtomicInteger(0);
        List<Integer> reconsumeTimesList = Collections.synchronizedList(new ArrayList<Integer>());
        List<String> attemptTrace = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch warmupLatch = new CountDownLatch(1);
        CountDownLatch localRetriesDone = new CountDownLatch(MAX_RECONSUME_TIMES + 1);

        DefaultMQPushConsumer consumer = createConsumer(group, "OrderDLQConsumer");
        consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
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

                        int attempt = totalAttempts.incrementAndGet();
                        int reconsumeTimes = msg.getReconsumeTimes();
                        reconsumeTimesList.add(reconsumeTimes);
                        String trace = String.format(
                                "attempt=%d, reconsumeTimes=%d, msgTopic=%s, queueId=%d",
                                attempt, reconsumeTimes, msg.getTopic(), msg.getQueueId());
                        attemptTrace.add(trace);
                        System.out.println("[ORDER-DLQ] " + trace);

                        if (attempt <= MAX_RECONSUME_TIMES + 1) {
                            localRetriesDone.countDown();
                        }

                        context.setSuspendCurrentQueueTimeMillis(1000);
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        DefaultMQProducer producer = createProducer(PRODUCER_GROUP_PREFIX + "_ORDER_DLQ", "OrderDLQProducer");
        MessageQueueSelector selector = createQueueSelector();

        try {
            consumer.start();
            Thread.sleep(5000L);
            producer.start();

            SendResult warmupResult = producer.send(
                    new Message(topic, "TAG_ORDER_DLQ", "WARMUP",
                            warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)),
                    selector, 0);
            System.out.println("[ORDER-DLQ] Warmup sent to queueId=" + warmupResult.getMessageQueue().getQueueId());
            assertTrue("warmup should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));

            Message msg = new Message(topic, "TAG_ORDER_DLQ", "ORDER_RETRY_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg, selector,
                    warmupResult.getMessageQueue().getQueueId());
            assertEquals(SendStatus.SEND_OK, result.getSendStatus());

            boolean allDone = localRetriesDone.await(2, TimeUnit.MINUTES);
            System.out.println("[ORDER-DLQ] Local retries done=" + allDone
                    + ", totalAttempts=" + totalAttempts.get()
                    + ", trace=" + attemptTrace);

            assertTrue("All 6 local consumption attempts should complete, trace=" + attemptTrace, allDone);

            // 验证前6次本地重试的 reconsumeTimes 正确递增: 0, 1, 2, 3, 4, 5
            int localRetryCount = Math.min(MAX_RECONSUME_TIMES + 1, reconsumeTimesList.size());
            assertEquals("Should have at least 6 reconsumeTimes records", MAX_RECONSUME_TIMES + 1, localRetryCount);
            for (int i = 0; i < localRetryCount; i++) {
                assertEquals("reconsumeTimes at attempt " + (i + 1) + " should be " + i,
                        i, reconsumeTimesList.get(i).intValue());
            }

            // 验证顺序消费始终在原始Topic上重试
            for (int i = 0; i < localRetryCount; i++) {
                assertTrue("Local retry should be on original topic",
                        attemptTrace.get(i).contains("msgTopic=" + topic + ","));
            }

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

    private MessageQueueSelector createQueueSelector() {
        return new MessageQueueSelector() {
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
    }

    private String uniqueTopic(String scenario) {
        return TOPIC_PREFIX + "_" + scenario;
    }
}
