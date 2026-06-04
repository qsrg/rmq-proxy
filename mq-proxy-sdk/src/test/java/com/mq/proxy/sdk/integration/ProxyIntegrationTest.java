package com.mq.proxy.sdk.integration;

import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.PullResult;
import com.mq.proxy.sdk.consumer.lite.ProxyLitePullConsumer;
import com.mq.proxy.sdk.consumer.model.DecodedMessage;
import com.mq.proxy.sdk.consumer.model.ProxyMessage;
import com.mq.proxy.sdk.consumer.push.ConsumeStatus;
import com.mq.proxy.sdk.consumer.push.ProxyMessageListener;
import com.mq.proxy.sdk.consumer.push.ProxyPushConsumer;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.producer.ProxyProducer;
import com.mq.proxy.sdk.producer.SendResult;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class ProxyIntegrationTest {

    private static EmbeddedProxy proxy;
    private static final String PROXY_ADDR = "127.0.0.1:19876";

    private static final String NATIVE_TOPIC = "NativeProxyIntegrationTopic_" + System.currentTimeMillis();
    private static final String SDK_TOPIC = "SDKProxyIntegrationTopic_" + System.currentTimeMillis();
    private static final String SDK_PUSH_TOPIC = "SDKPushProxyIntegrationTopic_" + System.currentTimeMillis();
    private static final String SDK_LITE_TOPIC = "SDKLiteProxyIntegrationTopic_" + System.currentTimeMillis();
    private static final String NATIVE_PRODUCER_GROUP = "NativeTestProducerGroup_" + System.currentTimeMillis();
    private static final String NATIVE_CONSUMER_GROUP = "NativeTestConsumerGroup_" + System.currentTimeMillis();
    private static final String SDK_PRODUCER_GROUP = "SDKTestProducerGroup_" + System.currentTimeMillis();
    private static final String SDK_CONSUMER_GROUP = "SDKTestConsumerGroup_" + System.currentTimeMillis();
    private static final String SDK_PUSH_CONSUMER_GROUP = "SDKPushTestConsumerGroup_" + System.currentTimeMillis();
    private static final String SDK_LITE_CONSUMER_GROUP = "SDKLiteTestConsumerGroup_" + System.currentTimeMillis();

    @BeforeClass
    public static void setUpProxy() throws Exception {
        proxy = new EmbeddedProxy();
        proxy.start();
        Thread.sleep(3000);
    }

    @AfterClass
    public static void tearDownProxy() {
        if (proxy != null) {
            proxy.shutdown();
        }
    }

    @Test
    public void testNativeRocketMQProduceAndConsumeThroughProxy() throws Exception {
        AtomicInteger receivedCount = new AtomicInteger(0);
        int totalMessages = 10;
        CountDownLatch latch = new CountDownLatch(totalMessages);
        List<String> receivedBodies = Collections.synchronizedList(new ArrayList<>());

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(NATIVE_CONSUMER_GROUP);
        consumer.setNamesrvAddr(PROXY_ADDR);
        consumer.subscribe(NATIVE_TOPIC, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    receivedCount.incrementAndGet();
                    try {
                        receivedBodies.add(new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        Thread.sleep(5000);

        DefaultMQProducer producer = new DefaultMQProducer(NATIVE_PRODUCER_GROUP);
        producer.setNamesrvAddr(PROXY_ADDR);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        producer.start();

        for (int i = 0; i < totalMessages; i++) {
            String body = "NativeMsg-" + i;
            Message msg = new Message(NATIVE_TOPIC, "TagA", body.getBytes(RemotingHelper.DEFAULT_CHARSET));
            org.apache.rocketmq.client.producer.SendResult result = producer.send(msg);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());
        }

        boolean allReceived = latch.await(60, TimeUnit.SECONDS);
        assertTrue("Should receive at least " + totalMessages + " messages, got " + receivedCount.get(),
                receivedCount.get() >= totalMessages);

        for (int i = 0; i < totalMessages; i++) {
            assertTrue("Should contain NativeMsg-" + i, receivedBodies.contains("NativeMsg-" + i));
        }

        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    public void testSDKProduceAndConsumeThroughProxy() throws Exception {
        String topic = SDK_TOPIC;
        String group = SDK_CONSUMER_GROUP;
        int queueId = 0;

        ProxyProducer producer = new ProxyProducer(SDK_PRODUCER_GROUP);
        producer.setProxyAddrs(PROXY_ADDR);
        producer.setRetryTimes(3);
        producer.setRequestTimeoutMillis(3000);
        producer.start();

        int totalMessages = 8;
        for (int i = 0; i < totalMessages; i++) {
            SendResult result = producer.send(topic, "TagA", "SDK-Msg-" + i, ("SDK-Body-" + i).getBytes());
            assertTrue("SDK send should succeed: " + (result.isSuccess() ? "OK" : result.getErrorMsg()), result.isSuccess());
        }

        producer.shutdown();

        ProxyConsumer consumer = new ProxyConsumer(group);
        consumer.setProxyAddrs(PROXY_ADDR);
        consumer.setRequestTimeoutMillis(3000);
        consumer.subscribe(topic, "*");
        consumer.start();

        long offset = consumer.queryConsumerOffset(group, topic, queueId);
        if (offset < 0) {
            offset = 0;
        }

        int consumed = 0;
        List<String> bodies = new ArrayList<>();
        int maxAttempts = 30;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            try {
                PullResult pullResult = consumer.pull(topic, group, queueId, offset, 32);
                if (pullResult != null && pullResult.isFound() && pullResult.getBody() != null) {
                    List<DecodedMessage> decodedMessages = DecodedMessage.decode(pullResult.getBody());
                    for (DecodedMessage dm : decodedMessages) {
                        if (dm.getBody() != null) {
                            bodies.add(new String(dm.getBody()));
                            consumed++;
                        }
                    }
                    offset = pullResult.getNextBeginOffset();
                    consumer.updateConsumerOffset(group, topic, queueId, offset);
                    if (consumed >= totalMessages) {
                        break;
                    }
                } else {
                    Thread.sleep(500);
                }
            } catch (ProxyException e) {
                Thread.sleep(500);
            }
        }

        consumer.shutdown();

        assertTrue("Should consume at least some messages, got " + consumed, consumed >= 1);
    }

    @Test
    public void testSDKConsumerRespectsTagSubscription() throws Exception {
        String topic = SDK_TOPIC + "_TagFilter";
        String group = SDK_CONSUMER_GROUP + "_TagFilter";
        int queueId = 0;

        ProxyProducer producer = new ProxyProducer(SDK_PRODUCER_GROUP + "_TagFilter");
        producer.setProxyAddrs(PROXY_ADDR);
        producer.setRetryTimes(3);
        producer.setRequestTimeoutMillis(3000);
        producer.start();

        SendResult matched1 = producer.send(topic, "TagA", "TagA-1", "TagA-Body-1".getBytes(), 0, queueId);
        SendResult unmatched = producer.send(topic, "TagB", "TagB-1", "TagB-Body-1".getBytes(), 0, queueId);
        SendResult matched2 = producer.send(topic, "TagA", "TagA-2", "TagA-Body-2".getBytes(), 0, queueId);
        assertTrue(matched1.isSuccess());
        assertTrue(unmatched.isSuccess());
        assertTrue(matched2.isSuccess());
        producer.shutdown();

        ProxyConsumer consumer = new ProxyConsumer(group);
        consumer.setProxyAddrs(PROXY_ADDR);
        consumer.setRequestTimeoutMillis(3000);
        consumer.subscribe(topic, "TagA");
        consumer.start();

        long offset = consumer.queryConsumerOffset(group, topic, queueId);
        if (offset < 0) {
            offset = 0;
        }

        List<String> bodies = new ArrayList<>();
        int maxAttempts = 20;
        for (int attempt = 0; attempt < maxAttempts && bodies.size() < 2; attempt++) {
            PullResult pullResult = consumer.pull(topic, group, queueId, offset, 32);
            if (pullResult != null && pullResult.isFound() && pullResult.getBody() != null) {
                List<DecodedMessage> decodedMessages = DecodedMessage.decode(pullResult.getBody());
                for (DecodedMessage dm : decodedMessages) {
                    if (dm.getBody() != null) {
                        bodies.add(new String(dm.getBody()));
                    }
                }
                offset = pullResult.getNextBeginOffset();
                consumer.updateConsumerOffset(group, topic, queueId, offset);
            } else {
                Thread.sleep(500);
            }
        }

        consumer.shutdown();

        assertEquals("Should only consume matching tag messages", 2, bodies.size());
        assertTrue(bodies.contains("TagA-Body-1"));
        assertTrue(bodies.contains("TagA-Body-2"));
        assertFalse("Should not consume unmatched tag message", bodies.contains("TagB-Body-1"));
    }

    @Test
    public void testSDKPushConsumerProduceAndConsumeThroughProxy() throws Exception {
        String topic = SDK_PUSH_TOPIC;
        String group = SDK_PUSH_CONSUMER_GROUP;

        ProxyProducer producer = new ProxyProducer(SDK_PRODUCER_GROUP);
        producer.setProxyAddrs(PROXY_ADDR);
        producer.setRetryTimes(3);
        producer.setRequestTimeoutMillis(3000);
        producer.start();

        int totalMessages = 6;
        for (int i = 0; i < totalMessages; i++) {
            SendResult result = producer.send(topic, "TagA", "Push-Msg-" + i, ("Push-Body-" + i).getBytes());
            assertTrue("SDK push send should succeed: " + (result.isSuccess() ? "OK" : result.getErrorMsg()), result.isSuccess());
        }

        producer.shutdown();

        Thread.sleep(2000);

        CountDownLatch latch = new CountDownLatch(totalMessages);
        List<String> receivedBodies = Collections.synchronizedList(new ArrayList<>());

        ProxyPushConsumer pushConsumer = new ProxyPushConsumer(group);
        pushConsumer.setProxyAddrs(PROXY_ADDR);
        pushConsumer.setRequestTimeoutMillis(3000);
        pushConsumer.subscribe(topic, "*");
        pushConsumer.registerMessageListener(new ProxyMessageListener() {
            @Override
            public ConsumeStatus consume(List<ProxyMessage> messages) {
                for (ProxyMessage msg : messages) {
                    receivedBodies.add(new String(msg.getBody()));
                    latch.countDown();
                }
                return ConsumeStatus.SUCCESS;
            }
        });
        pushConsumer.start();

        boolean allReceived = latch.await(120, TimeUnit.SECONDS);
        pushConsumer.shutdown();

        assertTrue("PushConsumer should receive at least " + totalMessages + " messages, got " + receivedBodies.size(),
                receivedBodies.size() >= totalMessages);

        for (int i = 0; i < totalMessages; i++) {
            assertTrue("Should contain Push-Body-" + i, receivedBodies.contains("Push-Body-" + i));
        }
    }

    @Test
    public void testSDKLitePullConsumerProduceAndConsumeThroughProxy() throws Exception {
        String topic = SDK_LITE_TOPIC;
        String group = SDK_LITE_CONSUMER_GROUP;

        ProxyProducer producer = new ProxyProducer(SDK_PRODUCER_GROUP);
        producer.setProxyAddrs(PROXY_ADDR);
        producer.setRetryTimes(3);
        producer.setRequestTimeoutMillis(3000);
        producer.start();

        int totalMessages = 5;
        for (int i = 0; i < totalMessages; i++) {
            SendResult result = producer.send(topic, "TagA", "Lite-Msg-" + i, ("Lite-Body-" + i).getBytes());
            assertTrue("SDK lite send should succeed: " + (result.isSuccess() ? "OK" : result.getErrorMsg()), result.isSuccess());
        }

        producer.shutdown();

        Thread.sleep(2000);

        ProxyLitePullConsumer liteConsumer = new ProxyLitePullConsumer(group);
        liteConsumer.setProxyAddrs(PROXY_ADDR);
        liteConsumer.setRequestTimeoutMillis(3000);
        liteConsumer.setRetryTimes(3);
        liteConsumer.setAutoCommit(true);
        liteConsumer.subscribe(topic, "*");
        liteConsumer.start();

        Thread.sleep(10000);

        List<String> receivedBodies = new ArrayList<>();
        int maxAttempts = 30;
        for (int attempt = 0; attempt < maxAttempts && receivedBodies.size() < totalMessages; attempt++) {
            List<ProxyMessage> messages = liteConsumer.poll(3000);
            for (ProxyMessage msg : messages) {
                receivedBodies.add(new String(msg.getBody()));
            }
        }

        liteConsumer.shutdown();

        assertTrue("LitePullConsumer should consume at least some messages, got " + receivedBodies.size(), receivedBodies.size() >= 1);
    }

    @Test
    public void testSDKConsumerSupportsRunningInfoQueryThroughProxy() throws Exception {
        String topic = SDK_TOPIC + "_RunningInfo";
        String group = SDK_CONSUMER_GROUP + "_RunningInfo";

        ProxyConsumer consumer = new ProxyConsumer(group);
        consumer.setProxyAddrs(PROXY_ADDR);
        consumer.setRequestTimeoutMillis(3000);
        consumer.subscribe(topic, "TagA || TagB");
        consumer.start();

        DefaultMQAdminExt adminExt = new DefaultMQAdminExt("sdkRunningInfoAdminGroup_" + System.currentTimeMillis());
        adminExt.setNamesrvAddr(PROXY_ADDR);

        try {
            adminExt.start();
            Thread.sleep(5000);

            ConsumerConnection connection = adminExt.examineConsumerConnectionInfo(group);
            assertNotNull("Consumer connection should be available for SDK consumer", connection);
            assertEquals("Should expose exactly one SDK consumer", 1, connection.getConnectionSet().size());

            String clientId = connection.getConnectionSet().iterator().next().getClientId();
            ConsumerRunningInfo runningInfo = adminExt.getConsumerRunningInfo(group, clientId, false);

            assertNotNull("SDK consumer should respond to running info query", runningInfo);
            assertFalse("Running info should contain subscribed topics", runningInfo.getSubscriptionSet().isEmpty());
            assertEquals("Should expose subscribed topic", topic,
                runningInfo.getSubscriptionSet().iterator().next().getTopic());
        } finally {
            adminExt.shutdown();
            consumer.shutdown();
        }
    }

    @Test
    public void testSDKLitePullConsumerRunningInfoIncludesAssignedQueues() throws Exception {
        String topic = SDK_LITE_TOPIC + "_RunningInfo";
        String group = SDK_LITE_CONSUMER_GROUP + "_RunningInfo";

        ProxyProducer producer = new ProxyProducer(SDK_PRODUCER_GROUP + "_RunningInfo");
        producer.setProxyAddrs(PROXY_ADDR);
        producer.setRetryTimes(3);
        producer.setRequestTimeoutMillis(3000);
        producer.start();
        assertTrue(producer.send(topic, "TagA", "Lite-RunningInfo", "Lite-RunningInfo-Body".getBytes()).isSuccess());
        producer.shutdown();

        ProxyLitePullConsumer liteConsumer = new ProxyLitePullConsumer(group);
        liteConsumer.setProxyAddrs(PROXY_ADDR);
        liteConsumer.setRequestTimeoutMillis(3000);
        liteConsumer.setRetryTimes(3);
        liteConsumer.setAutoCommit(true);
        liteConsumer.subscribe(topic, "*");
        liteConsumer.start();

        DefaultMQAdminExt adminExt = new DefaultMQAdminExt("sdkLiteRunningInfoAdminGroup_" + System.currentTimeMillis());
        adminExt.setNamesrvAddr(PROXY_ADDR);

        try {
            adminExt.start();
            Thread.sleep(8000);

            ConsumerConnection connection = adminExt.examineConsumerConnectionInfo(group);
            assertNotNull("Consumer connection should be available for SDK lite consumer", connection);
            assertEquals("Should expose exactly one SDK lite consumer", 1, connection.getConnectionSet().size());

            String clientId = connection.getConnectionSet().iterator().next().getClientId();
            ConsumerRunningInfo runningInfo = adminExt.getConsumerRunningInfo(group, clientId, false);

            assertNotNull("SDK lite consumer should respond to running info query", runningInfo);
            assertFalse("Running info should include assigned queues", runningInfo.getMqTable().isEmpty());
            assertTrue("Assigned queue should belong to subscribed topic",
                runningInfo.getMqTable().keySet().iterator().next().getTopic().equals(topic));
        } finally {
            adminExt.shutdown();
            liteConsumer.shutdown();
        }
    }
}
