package com.mq.proxy.rocketmq.integration;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
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

/**
 * 观察性测试：记录通过 Proxy 时并发消费重试的实际行为
 * 不做断言，仅打印观察结果，用于对比原生 RocketMQ 预期行为
 *
 * 预期行为（原生 RocketMQ, maxReconsumeTimes=3）：
 * - 总消费次数 = 4（1次原始 + 3次重试）
 * - reconsumeTimes 递增: 0, 1, 2, 3
 * - 重试间隔: 10s, 30s, 1m
 * - 第4次消费失败后进入 %DLQ% 死信队列
 */
public class ConsumeRetryObservationTest {

    private static final String NAMESRV_ADDR =
            System.getProperty("test.namesrvAddr", "127.0.0.1:9876");
    private static final int MAX_RECONSUME_TIMES = 3;

    private EmbeddedRocketMQProxy proxy;

    @Before
    public void setUp() throws Exception {
        proxy = new EmbeddedRocketMQProxy(NAMESRV_ADDR);
        proxy.start();
        System.out.println("=== Proxy started on " + proxy.getProxyAddr() + " ===\n");
    }

    @After
    public void tearDown() {
        if (proxy != null) {
            proxy.shutdown();
        }
    }

    /**
     * 观察通过 Proxy 的并发消费重试行为
     * 始终返回 RECONSUME_LATER，记录每次消费的详细信息
     */
    @Test
    public void observeConcurrentRetryBehavior() throws Exception {
        String topic = "OBSERVE_RETRY_" + System.currentTimeMillis();
        String group = "CID_OBSERVE_" + System.currentTimeMillis();
        String warmupBody = "observe-warmup-" + UUID.randomUUID();
        String retryBody = "observe-body-" + UUID.randomUUID();

        AtomicInteger totalAttempts = new AtomicInteger(0);
        List<String> observations = Collections.synchronizedList(new ArrayList<String>());
        CountDownLatch warmupLatch = new CountDownLatch(1);
        // 最多等待6次消费（预期4次，多等几次以观察异常行为）
        CountDownLatch observedEnough = new CountDownLatch(6);

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(proxy.getProxyAddr());
        consumer.setInstanceName("ObserveConsumer");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        consumer.setMaxReconsumeTimes(MAX_RECONSUME_TIMES);
        consumer.setPersistConsumerOffsetInterval(5000); // 每5秒持久化offset
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
                        long bornTimestamp = msg.getBornTimestamp();
                        long storeTimestamp = msg.getStoreTimestamp();
                        long now = System.currentTimeMillis();

                        String obs = String.format(
                                "attempt=%d, reconsumeTimes=%d, msgTopic=%s, RETRY_TOPIC_PROP=%s, " +
                                "queueId=%d, queueOffset=%d, bornTimestamp=%d, storeTimestamp=%d, now=%d, " +
                                "delayLevel=%d, msgId=%s, commitLogOffset=%d",
                                attempt, reconsumeTimes, msg.getTopic(), retryTopicProp,
                                msg.getQueueId(), msg.getQueueOffset(), bornTimestamp, storeTimestamp, now,
                                msg.getDelayTimeLevel(), msg.getMsgId(), msg.getCommitLogOffset());
                        observations.add(obs);
                        System.out.println("[OBSERVE] " + obs);

                        observedEnough.countDown();

                        // 始终返回 RECONSUME_LATER
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = new DefaultMQProducer("PID_OBSERVE_" + System.currentTimeMillis());
        producer.setNamesrvAddr(proxy.getProxyAddr());
        producer.setInstanceName("ObserveProducer");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        try {
            producer.start();
            // warmup
            producer.send(new Message(topic, "TAG_OBS", "WARMUP",
                    warmupBody.getBytes(RemotingHelper.DEFAULT_CHARSET)));

            consumer.start();
            System.out.println("[OBSERVE] Waiting for warmup...");
            assertTrue("warmup should be consumed", warmupLatch.await(60, TimeUnit.SECONDS));
            System.out.println("[OBSERVE] Warmup consumed. Sending test message...\n");

            // 发送测试消息
            Message msg = new Message(topic, "TAG_OBS", "OBSERVE_KEY",
                    retryBody.getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = producer.send(msg);
            System.out.println("[OBSERVE] Test message sent, sendStatus=" + result.getSendStatus()
                    + ", msgId=" + result.getMsgId()
                    + ", queueId=" + result.getMessageQueue().getQueueId()
                    + ", queueOffset=" + result.getQueueOffset());
            System.out.println();

            // 等待观察足够多的消费次数（最多5分钟）
            System.out.println("[OBSERVE] Waiting for consumption attempts (max 6, timeout 5min)...");
            System.out.println("[OBSERVE] Expected: 4 attempts with reconsumeTimes=0,1,2,3 then DLQ");
            System.out.println();

            boolean enough = observedEnough.await(5, TimeUnit.MINUTES);

            // 打印汇总
            System.out.println("\n========== OBSERVATION SUMMARY ==========");
            System.out.println("maxReconsumeTimes = " + MAX_RECONSUME_TIMES);
            System.out.println("Total attempts observed = " + totalAttempts.get());
            System.out.println("Timeout reached = " + !enough);
            System.out.println();

            System.out.println("--- reconsumeTimes sequence ---");
            StringBuilder rtSeq = new StringBuilder();
            for (String obs : observations) {
                String rt = obs.split("reconsumeTimes=")[1].split(",")[0];
                rtSeq.append(rt).append(", ");
            }
            System.out.println("reconsumeTimes: " + rtSeq.toString());
            System.out.println();

            System.out.println("--- Expected vs Actual ---");
            System.out.println("Expected reconsumeTimes: 0, 1, 2, 3 (then DLQ, total 4 attempts)");
            System.out.println("Expected retry intervals: 10s, 30s, 1m");
            System.out.println();

            // 计算实际间隔
            System.out.println("--- Actual intervals ---");
            for (int i = 1; i < observations.size(); i++) {
                long prevNow = Long.parseLong(observations.get(i - 1).split("now=")[1].split(",")[0]);
                long curNow = Long.parseLong(observations.get(i).split("now=")[1].split(",")[0]);
                long intervalMs = curNow - prevNow;
                System.out.println("Interval between attempt " + i + " and " + (i + 1) + ": " + intervalMs + "ms (" + (intervalMs / 1000) + "s)");
            }

            System.out.println("==========================================\n");

        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    private void assertTrue(String message, boolean condition) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
