package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class NativeClientIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "NATIVE_CLIENT_TEST";
    private static final String PRODUCER_GROUP = "PID_NATIVE_TEST";
    private static final String CONSUMER_GROUP = "CID_NATIVE_CONSUME_TEST";
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
        String proxyNamesrvAddr = proxy.getProxyAddr();

        NettyRemotingClient testClient = new NettyRemotingClient(new NettyClientConfig());
        testClient.start();

        try {
            com.mq.proxy.core.protocol.RemotingCommand request =
                com.mq.proxy.core.protocol.RemotingCommand.createRequestCommand(
                    com.mq.proxy.core.protocol.RequestCode.GET_ROUTEINFO_BY_TOPIC,
                    new com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader(TOPIC));
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
            testClient.shutdown();
        }
    }

    @Test
    public void testNativeProducerSendMessage() throws Exception {
        String proxyNamesrvAddr = proxy.getProxyAddr();

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(proxyNamesrvAddr);
        producer.setInstanceName("NativeProducerTest");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        try {
            producer.start();
            System.out.println("Native producer started, sending message...");

            Message msg = new Message(TOPIC, "TAG_NATIVE", "KEY_NATIVE",
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
    public void testNativeConsumerConsumeMessage() throws Exception {
        String proxyNamesrvAddr = proxy.getProxyAddr();
        String consumeTopic = TOPIC + "_CONSUME_" + System.currentTimeMillis();
        String consumeGroup = CONSUMER_GROUP + "_" + System.currentTimeMillis();
        int totalMessages = 5;

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP + "_CONSUME_TEST");
        producer.setNamesrvAddr(proxyNamesrvAddr);
        producer.setInstanceName("NativeConsumeProducerTest");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);
        producer.start();

        for (int i = 0; i < totalMessages; i++) {
            Message msg = new Message(consumeTopic, "TAG_CONSUME",
                    ("ConsumeTest-" + i).getBytes("UTF-8"));
            SendResult result = producer.send(msg);
            assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, result.getSendStatus());
            System.out.println("Sent message " + i + ", msgId=" + result.getMsgId());
        }
        producer.shutdown();
        System.out.println("Producer shutdown, starting consumer...");

        CountDownLatch latch = new CountDownLatch(totalMessages);
        AtomicInteger receivedCount = new AtomicInteger(0);
        List<String> receivedBodies = Collections.synchronizedList(new ArrayList<String>());

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumeGroup);
        consumer.setNamesrvAddr(proxyNamesrvAddr);
        consumer.setInstanceName("NativeConsumeConsumerTest");
        consumer.subscribe(consumeTopic, "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    receivedCount.incrementAndGet();
                    try {
                        receivedBodies.add(new String(msg.getBody(), "UTF-8"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        System.out.println("Consumer started, waiting for messages...");

        boolean allReceived = latch.await(60, TimeUnit.SECONDS);

        System.out.println("Received " + receivedCount.get() + "/" + totalMessages + " messages, waiting 30s for console inspection...");
        Thread.sleep(30000);

        consumer.shutdown();

        System.out.println("Received " + receivedCount.get() + "/" + totalMessages + " messages");
        for (String body : receivedBodies) {
            System.out.println("  Consumed: " + body);
        }

        assertTrue("Should receive at least " + totalMessages + " messages, got " + receivedCount.get(),
                receivedCount.get() >= totalMessages);

        for (int i = 0; i < totalMessages; i++) {
            assertTrue("Should contain ConsumeTest-" + i, receivedBodies.contains("ConsumeTest-" + i));
        }
    }

}
