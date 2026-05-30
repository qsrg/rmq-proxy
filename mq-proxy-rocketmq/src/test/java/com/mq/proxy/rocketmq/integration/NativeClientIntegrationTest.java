package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class NativeClientIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "NATIVE_CLIENT_TEST";
    private static final String PRODUCER_GROUP = "PID_NATIVE_TEST";
    private static final String CONSUMER_GROUP = "CID_NATIVE_TEST_" + System.currentTimeMillis();

    private NettyRemotingClient namesrvClient;
    private String brokerAddr;
    private NettyRemotingServer proxyServer;
    private int proxyPort;
    private StorageAdapter rocketmqAdapter;
    private MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;

    @Before
    public void setUp() throws Exception {
        namesrvClient = new NettyRemotingClient(new NettyClientConfig());
        namesrvClient.start();

        brokerAddr = RocketMQIntegrationTest.discoverBrokerAddr(namesrvClient, NAMESRV_ADDR);
        assertNotNull("Broker not found, is RocketMQ running on " + NAMESRV_ADDR + "?", brokerAddr);
        System.out.println("Discovered Broker: " + brokerAddr);

        proxyPort = findAvailablePort();

        StorageConfig rocketmqConfig = new StorageConfig();
        rocketmqConfig.setNamesrvAddr(NAMESRV_ADDR);
        rocketmqConfig.setConnectTimeoutMillis(5000);

        rocketmqAdapter = new RocketMQStorageAdapter();
        rocketmqAdapter.initialize(rocketmqConfig);

        messageEngine = new MessageEngine(rocketmqAdapter);

        virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.start(NAMESRV_ADDR, "127.0.0.1", proxyPort);
        messageEngine.setVirtualRouteManager(virtualRouteManager);

        clientConnectionManager = new ClientConnectionManager();
        heartbeatService = new ProxyBrokerHeartbeatService(clientConnectionManager, rocketmqAdapter, "127.0.0.1", proxyPort, virtualRouteManager);

        messageEngine.setOnSubscriptionNotLatest(() -> heartbeatService.sendHeartbeat());

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(proxyPort);
        proxyServer = new NettyRemotingServer(serverConfig);
        proxyServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(proxyServer);
        heartbeatService.setRemotingServer(proxyServer);
        clientConnectionManager.addClientInactiveListener(heartbeatService::unregisterClient);
        ProcessorRegister.registerProcessors(proxyServer, messageEngine, virtualRouteManager,
                clientConnectionManager, heartbeatService);
        proxyServer.start();
        heartbeatService.start();

        System.out.println("Proxy started on 127.0.0.1:" + proxyPort);
    }

    @After
    public void tearDown() {
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (proxyServer != null) {
            proxyServer.shutdown();
        }
        if (virtualRouteManager != null) {
            virtualRouteManager.shutdown();
        }
        if (rocketmqAdapter != null) {
            rocketmqAdapter.shutdown();
        }
        if (namesrvClient != null) {
            namesrvClient.shutdown();
        }
    }

    @Test
    public void testProxyGetRouteInfo() throws Exception {
        String proxyNamesrvAddr = "127.0.0.1:" + proxyPort;

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
        String proxyNamesrvAddr = "127.0.0.1:" + proxyPort;

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
    public void testNativeProducerAndConsumer() throws Exception {
        String proxyNamesrvAddr = "127.0.0.1:" + proxyPort;
        String uniqueTag = "TAG_NATIVE_CONSUME_" + System.currentTimeMillis();
        String uniqueKey = "KEY_NATIVE_CONSUME_" + System.currentTimeMillis();
        String msgBody = "Native client consume test: " + System.currentTimeMillis();

        CountDownLatch consumeLatch = new CountDownLatch(1);
        AtomicReference<MessageExt> receivedMsg = new AtomicReference<>();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(proxyNamesrvAddr);
        consumer.setInstanceName("NativeConsumerTest");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimeout(15);
        consumer.subscribe(TOPIC, uniqueTag);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.println("Received message: topic=" + msg.getTopic()
                                + ", tags=" + msg.getTags()
                                + ", keys=" + msg.getKeys()
                                + ", body=" + new String(msg.getBody(), "UTF-8"));
                    } catch (Exception e) {
                        System.out.println("Received message: topic=" + msg.getTopic()
                                + ", tags=" + msg.getTags()
                                + ", keys=" + msg.getKeys()
                                + ", body decode error");
                    }
                    if (uniqueTag.equals(msg.getTags()) && uniqueKey.equals(msg.getKeys())) {
                        receivedMsg.set(msg);
                        consumeLatch.countDown();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(proxyNamesrvAddr);
        producer.setInstanceName("NativeProducerTest2");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        try {
            consumer.start();
            System.out.println("Native consumer started, waiting for rebalance...");
            System.out.println("Consumer namesrvAddr: " + consumer.getNamesrvAddr());
            System.out.println("Consumer group: " + CONSUMER_GROUP);
            System.out.println("Consumer subscribed topic: " + TOPIC + " with tag: " + uniqueTag);
            Thread.sleep(10000);

            producer.start();
            System.out.println("Native producer started, sending tagged message...");

            Message msg = new Message(TOPIC, uniqueTag, uniqueKey, msgBody.getBytes("UTF-8"));
            SendResult sendResult = producer.send(msg);
            System.out.println("Send result: " + sendResult);

            assertNotNull("SendResult should not be null", sendResult);
            assertEquals("Send status should be SEND_OK",
                    org.apache.rocketmq.client.producer.SendStatus.SEND_OK,
                    sendResult.getSendStatus());

            boolean consumed = consumeLatch.await(30, TimeUnit.SECONDS);
            if (consumed) {
                MessageExt received = receivedMsg.get();
                assertNotNull("Received message should not be null", received);
                assertEquals("Message topic should match", TOPIC, received.getTopic());
                assertEquals("Message tags should match", uniqueTag, received.getTags());
                assertEquals("Message body should match", msgBody, new String(received.getBody(), "UTF-8"));
                System.out.println("SUCCESS: Native client produced and consumed message through proxy!");
            } else {
                fail("Message was not consumed within 30 seconds");
            }
        } finally {
            producer.shutdown();
        }
    }

    @Test
    public void testNativeConsumersRebalanceAfterOneShutdown() throws Exception {
        String proxyNamesrvAddr = "127.0.0.1:" + proxyPort;
        String group = "CID_NATIVE_REBALANCE_" + System.currentTimeMillis();
        String tag = "TAG_NATIVE_REBALANCE_" + System.currentTimeMillis();

        CountDownLatch firstPhaseLatch = new CountDownLatch(8);
        CountDownLatch secondPhaseLatch = new CountDownLatch(4);
        Set<String> firstPhaseBodies = Collections.synchronizedSet(new java.util.HashSet<String>());
        Set<String> secondPhaseBodies = Collections.synchronizedSet(new java.util.HashSet<String>());

        DefaultMQPushConsumer consumer1 = createConsumer(proxyNamesrvAddr, group, "NativeRebalanceConsumer1", tag,
                firstPhaseLatch, secondPhaseLatch, firstPhaseBodies, secondPhaseBodies);
        DefaultMQPushConsumer consumer2 = createConsumer(proxyNamesrvAddr, group, "NativeRebalanceConsumer2", tag,
                firstPhaseLatch, secondPhaseLatch, firstPhaseBodies, secondPhaseBodies);

        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(proxyNamesrvAddr);
        producer.setInstanceName("NativeRebalanceProducer");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        try {
            consumer1.start();
            consumer2.start();
            Thread.sleep(10000);

            producer.start();

            for (int i = 0; i < 8; i++) {
                String body = "rebalance-phase1-" + i;
                SendResult sendResult = producer.send(new Message(TOPIC, tag, body.getBytes("UTF-8")));
                assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, sendResult.getSendStatus());
            }

            assertTrue("Both consumers should finish first phase consumption",
                    firstPhaseLatch.await(30, TimeUnit.SECONDS));
            assertEquals(8, firstPhaseBodies.size());

            consumer2.shutdown();
            Thread.sleep(10000);

            for (int i = 0; i < 4; i++) {
                String body = "rebalance-phase2-" + i;
                SendResult sendResult = producer.send(new Message(TOPIC, tag, body.getBytes("UTF-8")));
                assertEquals(org.apache.rocketmq.client.producer.SendStatus.SEND_OK, sendResult.getSendStatus());
            }

            assertTrue("Remaining consumer should continue consuming after rebalance",
                    secondPhaseLatch.await(30, TimeUnit.SECONDS));
            assertEquals(4, secondPhaseBodies.size());
        } finally {
            producer.shutdown();
            try {
                consumer1.shutdown();
            } catch (Exception ignore) {
            }
            try {
                consumer2.shutdown();
            } catch (Exception ignore) {
            }
        }
    }

    private DefaultMQPushConsumer createConsumer(String proxyNamesrvAddr,
                                                 String group,
                                                 String instanceName,
                                                 String tag,
                                                 CountDownLatch firstPhaseLatch,
                                                 CountDownLatch secondPhaseLatch,
                                                 Set<String> firstPhaseBodies,
                                                 Set<String> secondPhaseBodies) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr(proxyNamesrvAddr);
        consumer.setInstanceName(instanceName);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimeout(15);
        consumer.subscribe(TOPIC, tag);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        String body = new String(msg.getBody(), "UTF-8");
                        if (body.startsWith("rebalance-phase1-")) {
                            if (firstPhaseBodies.add(body)) {
                                firstPhaseLatch.countDown();
                            }
                        } else if (body.startsWith("rebalance-phase2-")) {
                            if (secondPhaseBodies.add(body)) {
                                secondPhaseLatch.countDown();
                            }
                        }
                    } catch (Exception ignore) {
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        return consumer;
    }

    private int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }
}
