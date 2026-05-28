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

import java.io.File;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class TlsIntegrationTest {

    private static final String KEYSTORE_PATH = "/tmp/proxy-test-keystore.jks";
    private static final String TRUSTSTORE_PATH = "/tmp/proxy-test-truststore.jks";
    private static final String CERT_PATH = "/tmp/proxy-test-cert.cer";
    private static final String STORE_PASSWORD = "testpass";

    private int port;
    private StorageAdapterManager storageAdapterManager;
    private NettyRemotingServer remotingServer;
    private NettyRemotingClient tlsClient;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;

    @Before
    public void setUp() throws Exception {
        generateCertificates();

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
        nettyServerConfig.setTlsEnabled(true);
        nettyServerConfig.setTlsKeyStorePath(KEYSTORE_PATH);
        nettyServerConfig.setTlsKeyStorePassword(STORE_PASSWORD);

        remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(remotingServer);
        ProcessorRegister.registerProcessors(remotingServer, messageEngine, virtualRouteManager, clientConnectionManager);

        remotingServer.start();
        heartbeatService.start();

        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setTlsEnabled(true);
        clientConfig.setTlsTrustStorePath(TRUSTSTORE_PATH);
        clientConfig.setTlsTrustStorePassword(STORE_PASSWORD);
        tlsClient = new NettyRemotingClient(clientConfig);
        tlsClient.start();
    }

    @After
    public void tearDown() {
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (tlsClient != null) {
            tlsClient.shutdown();
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
        deleteFile(KEYSTORE_PATH);
        deleteFile(TRUSTSTORE_PATH);
        deleteFile(CERT_PATH);
    }

    private void generateCertificates() throws Exception {
        ProcessBuilder pb1 = new ProcessBuilder(
                "keytool", "-genkeypair", "-alias", "proxy", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "1", "-keystore", KEYSTORE_PATH, "-storepass", STORE_PASSWORD,
                "-keypass", STORE_PASSWORD,
                "-dname", "CN=proxy-test,OU=test,O=test,L=test,ST=test,C=CN",
                "-deststoretype", "JKS"
        );
        pb1.inheritIO();
        Process p1 = pb1.start();
        int code1 = p1.waitFor();
        assertEquals("keytool genkeypair failed", 0, code1);

        ProcessBuilder pb2 = new ProcessBuilder(
                "keytool", "-exportcert", "-alias", "proxy", "-keystore", KEYSTORE_PATH,
                "-storepass", STORE_PASSWORD, "-file", CERT_PATH
        );
        pb2.inheritIO();
        Process p2 = pb2.start();
        int code2 = p2.waitFor();
        assertEquals("keytool exportcert failed", 0, code2);

        ProcessBuilder pb3 = new ProcessBuilder(
                "keytool", "-importcert", "-alias", "proxy", "-file", CERT_PATH,
                "-keystore", TRUSTSTORE_PATH, "-storepass", STORE_PASSWORD, "-noprompt"
        );
        pb3.inheritIO();
        Process p3 = pb3.start();
        int code3 = p3.waitFor();
        assertEquals("keytool importcert failed", 0, code3);
    }

    private void deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testTlsSendMessage() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "tlsTestProducerGroup");
        extFields.put("topic", "tlsTestTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        request.setExtFields(extFields);
        request.setBody("tls test message body".getBytes());

        RemotingCommand response = tlsClient.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getExtFields());
        assertNotNull(response.getExtFields().get("msgId"));
        assertNotNull(response.getExtFields().get("queueId"));
        assertNotNull(response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testTlsPullMessage() throws Exception {
        RemotingCommand sendRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> sendExtFields = new HashMap<>();
        sendExtFields.put("producerGroup", "tlsTestProducerGroup");
        sendExtFields.put("topic", "tlsPullTestTopic");
        sendExtFields.put("defaultTopic", "defaultTopic");
        sendExtFields.put("defaultTopicQueueNums", "4");
        sendExtFields.put("queueId", "0");
        sendExtFields.put("sysFlag", "0");
        sendExtFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        sendExtFields.put("flag", "0");
        sendRequest.setExtFields(sendExtFields);
        sendRequest.setBody("tls test pull message".getBytes());
        tlsClient.invokeSync("127.0.0.1:" + port, sendRequest, 5000);

        RemotingCommand pullRequest = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> pullExtFields = new HashMap<>();
        pullExtFields.put("consumerGroup", "tlsTestConsumerGroup");
        pullExtFields.put("topic", "tlsPullTestTopic");
        pullExtFields.put("queueId", "0");
        pullExtFields.put("queueOffset", "0");
        pullExtFields.put("maxMsgNums", "32");
        pullExtFields.put("sysFlag", "0");
        pullExtFields.put("suspendTimeoutMillis", "0");
        pullRequest.setExtFields(pullExtFields);

        RemotingCommand pullResponse = tlsClient.invokeSync("127.0.0.1:" + port, pullRequest, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, pullResponse.getCode());
        assertNotNull(pullResponse.getExtFields());
        assertNotNull(pullResponse.getExtFields().get("nextBeginOffset"));
        assertNotNull(pullResponse.getExtFields().get("minOffset"));
        assertNotNull(pullResponse.getExtFields().get("maxOffset"));
    }

    @Test
    public void testTlsHeartBeat() throws Exception {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID("tlsTestClientId");

        Set<ProducerData> producerDataSet = new HashSet<>();
        ProducerData producerData = new ProducerData();
        producerData.setGroupName("tlsTestProducerGroup");
        producerDataSet.add(producerData);
        heartbeatData.setProducerDataSet(producerDataSet);

        Set<ConsumerData> consumerDataSet = new HashSet<>();
        ConsumerData consumerData = new ConsumerData();
        consumerData.setGroupName("tlsTestConsumerGroup");
        consumerDataSet.add(consumerData);
        heartbeatData.setConsumerDataSet(consumerDataSet);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeatData.encode());

        RemotingCommand response = tlsClient.invokeSync("127.0.0.1:" + port, request, 5000);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testNonTlsClientCannotConnect() throws Exception {
        NettyClientConfig nonTlsClientConfig = new NettyClientConfig();
        NettyRemotingClient nonTlsClient = new NettyRemotingClient(nonTlsClientConfig);
        nonTlsClient.start();

        try {
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
            HashMap<String, String> extFields = new HashMap<>();
            extFields.put("producerGroup", "nonTlsProducerGroup");
            extFields.put("topic", "nonTlsTestTopic");
            extFields.put("defaultTopic", "defaultTopic");
            extFields.put("defaultTopicQueueNums", "4");
            extFields.put("queueId", "0");
            extFields.put("sysFlag", "0");
            extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
            extFields.put("flag", "0");
            request.setExtFields(extFields);
            request.setBody("non-tls message".getBytes());

            try {
                nonTlsClient.invokeSync("127.0.0.1:" + port, request, 3000);
                fail("Non-TLS client should not be able to connect to TLS-enabled server");
            } catch (Exception e) {
                // Expected: non-TLS client cannot communicate with TLS server
            }
        } finally {
            nonTlsClient.shutdown();
        }
    }
}
