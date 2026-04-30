package com.mq.proxy.core.integration;

import com.mq.proxy.core.config.ProxyConfig;
import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData.ConsumerData;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData.ProducerData;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.StorageConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class ProxyIntegrationTest {

    private int port;
    private StorageAdapterManager storageAdapterManager;
    private NettyRemotingServer remotingServer;
    private NettyRemotingClient client;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;

    @Before
    public void setUp() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        port = ss.getLocalPort();
        ss.close();

        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setListenPort(port);
        proxyConfig.setStorageAdapterType("mock");

        storageAdapterManager = new StorageAdapterManager();
        TestMockStorageAdapter mockStorageAdapter = new TestMockStorageAdapter();
        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setAdapterType("mock");
        mockStorageAdapter.initialize(storageConfig);
        storageAdapterManager.registerAdapter("mock", mockStorageAdapter, true);

        MessageEngine messageEngine = new MessageEngine(storageAdapterManager);

        virtualRouteManager = new VirtualRouteManager();

        clientConnectionManager = new ClientConnectionManager();
        heartbeatService = new ProxyBrokerHeartbeatService(clientConnectionManager, storageAdapterManager, "127.0.0.1", port);

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(port);

        remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(remotingServer);
        ProcessorRegister.registerProcessors(remotingServer, messageEngine, virtualRouteManager, clientConnectionManager);

        remotingServer.start();
        heartbeatService.start();

        NettyClientConfig clientConfig = new NettyClientConfig();
        client = new NettyRemotingClient(clientConfig);
        client.start();
    }

    @After
    public void tearDown() {
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (client != null) {
            client.shutdown();
        }
        if (remotingServer != null) {
            remotingServer.shutdown();
        }
        if (storageAdapterManager != null) {
            storageAdapterManager.shutdownAll();
        }
        if (virtualRouteManager != null) {
            virtualRouteManager.shutdown();
        }
    }

    @Test
    public void testSendMessage() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("topic", "testTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        request.setExtFields(extFields);
        request.setBody("test message body".getBytes());

        RemotingCommand response = client.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getExtFields());
        assertNotNull(response.getExtFields().get("msgId"));
        assertNotNull(response.getExtFields().get("queueId"));
        assertNotNull(response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testSendMessageV2() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", "testProducerGroup");
        extFields.put("b", "testTopicV2");
        extFields.put("c", "defaultTopic");
        extFields.put("d", "4");
        extFields.put("e", "0");
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        request.setExtFields(extFields);
        request.setBody("test message v2 body".getBytes());

        RemotingCommand response = client.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getExtFields());
        assertNotNull(response.getExtFields().get("msgId"));
        assertNotNull(response.getExtFields().get("queueId"));
        assertNotNull(response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testPullMessage() throws Exception {
        RemotingCommand sendRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> sendExtFields = new HashMap<>();
        sendExtFields.put("producerGroup", "testProducerGroup");
        sendExtFields.put("topic", "pullTestTopic");
        sendExtFields.put("defaultTopic", "defaultTopic");
        sendExtFields.put("defaultTopicQueueNums", "4");
        sendExtFields.put("queueId", "0");
        sendExtFields.put("sysFlag", "0");
        sendExtFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        sendExtFields.put("flag", "0");
        sendRequest.setExtFields(sendExtFields);
        sendRequest.setBody("test pull message".getBytes());
        client.invokeSync("127.0.0.1:" + port, sendRequest, 5000);

        RemotingCommand pullRequest = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> pullExtFields = new HashMap<>();
        pullExtFields.put("consumerGroup", "testConsumerGroup");
        pullExtFields.put("topic", "pullTestTopic");
        pullExtFields.put("queueId", "0");
        pullExtFields.put("queueOffset", "0");
        pullExtFields.put("maxMsgNums", "32");
        pullExtFields.put("sysFlag", "0");
        pullExtFields.put("suspendTimeoutMillis", "0");
        pullRequest.setExtFields(pullExtFields);

        RemotingCommand pullResponse = client.invokeSync("127.0.0.1:" + port, pullRequest, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, pullResponse.getCode());
        assertNotNull(pullResponse.getExtFields());
        assertNotNull(pullResponse.getExtFields().get("nextBeginOffset"));
        assertNotNull(pullResponse.getExtFields().get("minOffset"));
        assertNotNull(pullResponse.getExtFields().get("maxOffset"));
    }

    @Test
    public void testQueryAndUpdateConsumerOffset() throws Exception {
        RemotingCommand sendRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> sendExtFields = new HashMap<>();
        sendExtFields.put("producerGroup", "testProducerGroup");
        sendExtFields.put("topic", "offsetTestTopic");
        sendExtFields.put("defaultTopic", "defaultTopic");
        sendExtFields.put("defaultTopicQueueNums", "4");
        sendExtFields.put("queueId", "0");
        sendExtFields.put("sysFlag", "0");
        sendExtFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        sendExtFields.put("flag", "0");
        sendRequest.setExtFields(sendExtFields);
        sendRequest.setBody("test offset message".getBytes());
        client.invokeSync("127.0.0.1:" + port, sendRequest, 5000);

        RemotingCommand updateRequest = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, null);
        HashMap<String, String> updateExtFields = new HashMap<>();
        updateExtFields.put("consumerGroup", "testConsumerGroup");
        updateExtFields.put("topic", "offsetTestTopic");
        updateExtFields.put("queueId", "0");
        updateExtFields.put("commitOffset", "0");
        updateRequest.setExtFields(updateExtFields);

        RemotingCommand updateResponse = client.invokeSync("127.0.0.1:" + port, updateRequest, 5000);
        assertEquals(RemotingSysResponseCode.SUCCESS, updateResponse.getCode());

        RemotingCommand queryRequest = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, null);
        HashMap<String, String> queryExtFields = new HashMap<>();
        queryExtFields.put("consumerGroup", "testConsumerGroup");
        queryExtFields.put("topic", "offsetTestTopic");
        queryExtFields.put("queueId", "0");
        queryRequest.setExtFields(queryExtFields);

        RemotingCommand queryResponse = client.invokeSync("127.0.0.1:" + port, queryRequest, 5000);
        assertEquals(RemotingSysResponseCode.SUCCESS, queryResponse.getCode());
        assertNotNull(queryResponse.getExtFields());
        assertEquals("0", queryResponse.getExtFields().get("offset"));
    }

    @Test
    public void testHeartBeat() throws Exception {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID("testClientId");

        Set<ProducerData> producerDataSet = new HashSet<>();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName("testProducerGroup");
        producerDataSet.add(producerData);
        heartbeatData.setProducerDataSet(producerDataSet);

        Set<ConsumerData> consumerDataSet = new HashSet<>();
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("testConsumerGroup");
        consumerDataSet.add(consumerData);
        heartbeatData.setConsumerDataSet(consumerDataSet);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeatData.encode());

        RemotingCommand response = client.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testUnregisterClient() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("clientID", "testClientId");
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("consumerGroup", "testConsumerGroup");
        request.setExtFields(extFields);

        RemotingCommand response = client.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testUnsupportedRequestCode() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(999, null);
        request.setBody("test".getBytes());

        RemotingCommand response = client.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, response.getCode());
    }
}
