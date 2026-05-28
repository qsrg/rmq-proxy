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
import com.mq.proxy.core.storage.StorageAdapterManager;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class NativeClientIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "NATIVE_CLIENT_TEST";
    private static final String PRODUCER_GROUP = "PID_NATIVE_TEST";
    private static final String CONSUMER_GROUP = "CID_NATIVE_TEST";

    private NettyRemotingClient namesrvClient;
    private String brokerAddr;
    private NettyRemotingServer proxyServer;
    private int proxyPort;
    private StorageAdapterManager storageAdapterManager;
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

        storageAdapterManager = new StorageAdapterManager();
        StorageConfig rocketmqConfig = new StorageConfig();
        rocketmqConfig.setAdapterType("rocketmq");
        rocketmqConfig.setNamesrvAddr(NAMESRV_ADDR);
        rocketmqConfig.setBrokerAddr(brokerAddr);
        rocketmqConfig.setConnectTimeoutMillis(5000);

        RocketMQStorageAdapter rocketmqAdapter = new RocketMQStorageAdapter();
        rocketmqAdapter.initialize(rocketmqConfig);
        storageAdapterManager.registerAdapter("rocketmq", rocketmqAdapter, true);

        messageEngine = new MessageEngine(storageAdapterManager);

        virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.start(NAMESRV_ADDR, "127.0.0.1", proxyPort);
        messageEngine.setVirtualRouteManager(virtualRouteManager);

        clientConnectionManager = new ClientConnectionManager();
        heartbeatService = new ProxyBrokerHeartbeatService(clientConnectionManager, storageAdapterManager, "127.0.0.1", proxyPort);

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(proxyPort);
        proxyServer = new NettyRemotingServer(serverConfig);
        proxyServer.setClientConnectionManager(clientConnectionManager);
        ProcessorRegister.registerProcessors(proxyServer, messageEngine, virtualRouteManager, clientConnectionManager);
        proxyServer.start();
        messageEngine.setRemotingServer(proxyServer);
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
        if (storageAdapterManager != null) {
            storageAdapterManager.shutdownAll();
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
            //consumer.shutdown();
        }
    }

    private int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }
}