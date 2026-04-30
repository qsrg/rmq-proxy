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
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class ConsumerDebugTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "CONSUMER_DEBUG_TEST";
    private static final String PRODUCER_GROUP = "PID_DEBUG_TEST";
    private static final String CONSUMER_GROUP = "CID_DEBUG_TEST";

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
        String logDir = "F:" + File.separator + "logs" + File.separator + "rocketmqlogs";
        System.setProperty(ClientLogger.CLIENT_LOG_ROOT, logDir);
        System.setProperty(ClientLogger.CLIENT_LOG_LEVEL, "DEBUG");
        System.out.println("=== Consumer log directory set to: " + logDir + " ===");

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
    public void testConsumerPullMessage() throws Exception {
        String proxyNamesrvAddr = "127.0.0.1:" + proxyPort;
        String uniqueTag = "TAG_DEBUG_" + System.currentTimeMillis();
        String uniqueKey = "KEY_DEBUG_" + System.currentTimeMillis();
        String msgBody = "Debug test message: " + System.currentTimeMillis();

        CountDownLatch consumeLatch = new CountDownLatch(1);
        AtomicReference<MessageExt> receivedMsg = new AtomicReference<>();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(proxyNamesrvAddr);
        consumer.setInstanceName("DebugConsumerTest");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimeout(15);
        consumer.subscribe(TOPIC, uniqueTag);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println("=== CONSUMER RECEIVED " + msgs.size() + " MESSAGES ===");
                for (MessageExt msg : msgs) {
                    System.out.println("Message: topic=" + msg.getTopic()
                            + ", tags=" + msg.getTags()
                            + ", keys=" + msg.getKeys()
                            + ", body=" + new String(msg.getBody()));
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
        producer.setInstanceName("DebugProducerTest");
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendFailed(0);

        try {
            System.out.println("\n=== STEP 1: Starting consumer ===");
            System.out.println("Consumer namesrvAddr: " + proxyNamesrvAddr);
            System.out.println("Consumer group: " + CONSUMER_GROUP);
            System.out.println("Consumer subscribed topic: " + TOPIC + " with tag: " + uniqueTag);
            consumer.start();
            System.out.println("Consumer started, waiting for rebalance...");
            Thread.sleep(15000);

            System.out.println("\n=== STEP 2: Starting producer and sending message ===");
            producer.start();

            Message msg = new Message(TOPIC, uniqueTag, uniqueKey, msgBody.getBytes("UTF-8"));
            SendResult sendResult = producer.send(msg);
            System.out.println("Message sent successfully: " + sendResult);

            System.out.println("\n=== STEP 3: Waiting for message consumption ===");
            boolean consumed = consumeLatch.await(60, TimeUnit.SECONDS);

            if (consumed) {
                System.out.println("=== SUCCESS: Message consumed! ===");
                MessageExt received = receivedMsg.get();
                assertNotNull("Received message should not be null", received);
                assertEquals("Message topic should match", TOPIC, received.getTopic());
                assertEquals("Message tags should match", uniqueTag, received.getTags());
                assertEquals("Message body should match", msgBody, new String(received.getBody(), "UTF-8"));
            } else {
                System.out.println("=== FAILURE: Message not consumed within timeout ===");
                System.out.println("=== Check consumer logs at: " + System.getProperty(ClientLogger.CLIENT_LOG_ROOT) + " ===");
                fail("Message was not consumed within 60 seconds");
            }
        } finally {
            producer.shutdown();
            consumer.shutdown();
        }
    }

    private int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }
}
