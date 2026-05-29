package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.util.HashMap;

import static org.junit.Assert.*;

public class NativeClientTlsIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "TLS_NATIVE_CLIENT_TEST";
    private static final String PRODUCER_GROUP = "PID_TLS_NATIVE_TEST";
    private static final String CONSUMER_GROUP = "CID_TLS_NATIVE_TEST";

    private static final String CERT_PATH = "/tmp/proxy-tls-server.crt";
    private static final String KEY_PATH = "/tmp/proxy-tls-server.key";

    private NettyRemotingClient namesrvClient;
    private String brokerAddr;
    private NettyRemotingServer proxyServer;
    private int proxyPort;
    private StorageAdapter rocketmqAdapter;
    private MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;
    private NettyRemotingClient tlsClient;

    @Before
    public void setUp() throws Exception {
        generateCertificates();

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
        heartbeatService = new ProxyBrokerHeartbeatService(clientConnectionManager, rocketmqAdapter, "127.0.0.1", proxyPort);

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(proxyPort);
        serverConfig.setTlsEnabled(true);
        serverConfig.setTlsCertPath(CERT_PATH);
        serverConfig.setTlsKeyPath(KEY_PATH);

        proxyServer = new NettyRemotingServer(serverConfig);
        proxyServer.setClientConnectionManager(clientConnectionManager);
        ProcessorRegister.registerProcessors(proxyServer, messageEngine, virtualRouteManager, clientConnectionManager);
        proxyServer.start();
        messageEngine.setRemotingServer(proxyServer);
        heartbeatService.start();

        NettyClientConfig tlsClientConfig = new NettyClientConfig();
        tlsClientConfig.setTlsEnabled(true);
        tlsClientConfig.setTlsTrustCertPath(CERT_PATH);
        tlsClient = new NettyRemotingClient(tlsClientConfig);
        tlsClient.start();

        System.out.println("TLS Proxy started on 127.0.0.1:" + proxyPort);
    }

    @After
    public void tearDown() {
        if (tlsClient != null) {
            tlsClient.shutdown();
        }
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
        deleteFile(CERT_PATH);
        deleteFile(KEY_PATH);
    }

    private void generateCertificates() throws Exception {
        ProcessBuilder pb1 = new ProcessBuilder(
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", KEY_PATH, "-out", CERT_PATH, "-days", "1", "-nodes",
                "-subj", "/CN=proxy-test/OU=test/O=test/L=test/ST=test/C=CN"
        );
        pb1.inheritIO();
        Process p1 = pb1.start();
        int code1 = p1.waitFor();
        assertEquals("openssl cert generation failed", 0, code1);
    }

    private void deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void testTlsProxyRouteInfo() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", TOPIC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setExtFields(extFields);

        System.out.println("Sending TLS route request to proxy at " + proxyAddr);
        RemotingCommand response = tlsClient.invokeSync(proxyAddr, request, 5000);

        System.out.println("Route response code: " + response.getCode());
        System.out.println("Route response body: " + (response.getBody() != null ?
                new String(response.getBody(), java.nio.charset.StandardCharsets.UTF_8) : "null"));

        assertEquals("Route response should be SUCCESS",
                RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull("Route response body should not be null", response.getBody());
    }

    @Test
    public void testTlsProxySendMessage() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", PRODUCER_GROUP);
        extFields.put("b", TOPIC);
        extFields.put("c", "TBW102");
        extFields.put("d", "4");
        extFields.put("e", String.valueOf(-1));
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        extFields.put("j", "0");
        extFields.put("k", "false");
        extFields.put("l", "16");
        extFields.put("m", "false");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        request.setExtFields(extFields);
        request.setBody("Hello from TLS client through proxy!".getBytes("UTF-8"));

        System.out.println("Sending TLS message to proxy at " + proxyAddr);
        RemotingCommand response = tlsClient.invokeSync(proxyAddr, request, 10000);

        System.out.println("Send response: code=" + response.getCode() + ", remark=" + response.getRemark()
                + ", extFields=" + response.getExtFields());

        assertEquals("Send response should be SUCCESS",
                RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull("msgId should not be null", response.getExtFields().get("msgId"));
        assertNotNull("queueId should not be null", response.getExtFields().get("queueId"));
        assertNotNull("queueOffset should not be null", response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testTlsProxyPullMessage() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> sendExtFields = new HashMap<>();
        sendExtFields.put("a", PRODUCER_GROUP);
        sendExtFields.put("b", TOPIC);
        sendExtFields.put("c", "TBW102");
        sendExtFields.put("d", "4");
        sendExtFields.put("e", String.valueOf(-1));
        sendExtFields.put("f", "0");
        sendExtFields.put("g", String.valueOf(System.currentTimeMillis()));
        sendExtFields.put("h", "0");
        sendExtFields.put("j", "0");
        sendExtFields.put("k", "false");
        sendExtFields.put("l", "16");
        sendExtFields.put("m", "false");

        RemotingCommand sendRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        sendRequest.setExtFields(sendExtFields);
        sendRequest.setBody("TLS pull test message".getBytes("UTF-8"));

        RemotingCommand sendResponse = tlsClient.invokeSync(proxyAddr, sendRequest, 10000);
        assertEquals("Send should succeed before pull test", RemotingSysResponseCode.SUCCESS, sendResponse.getCode());

        int queueId = Integer.parseInt(sendResponse.getExtFields().get("queueId"));
        long queueOffset = Long.parseLong(sendResponse.getExtFields().get("queueOffset"));

        Thread.sleep(500);

        HashMap<String, String> pullExtFields = new HashMap<>();
        pullExtFields.put("consumerGroup", CONSUMER_GROUP);
        pullExtFields.put("topic", TOPIC);
        pullExtFields.put("queueId", String.valueOf(queueId));
        pullExtFields.put("queueOffset", String.valueOf(queueOffset));
        pullExtFields.put("maxMsgNums", "32");
        pullExtFields.put("sysFlag", "0");
        pullExtFields.put("commitOffset", "0");
        pullExtFields.put("suspendTimeoutMillis", "0");
        pullExtFields.put("subVersion", String.valueOf(System.currentTimeMillis()));
        pullExtFields.put("expressionType", "TAG");

        RemotingCommand pullRequest = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        pullRequest.setExtFields(pullExtFields);

        System.out.println("Sending TLS pull request to proxy at " + proxyAddr);
        RemotingCommand pullResponse = tlsClient.invokeSync(proxyAddr, pullRequest, 10000);

        System.out.println("Pull response: code=" + pullResponse.getCode()
                + ", bodyLen=" + (pullResponse.getBody() != null ? pullResponse.getBody().length : 0));

        assertTrue("Pull should return SUCCESS or PULL_NOT_FOUND",
                pullResponse.getCode() == RemotingSysResponseCode.SUCCESS
                        || pullResponse.getCode() == com.mq.proxy.core.protocol.ResponseCode.PULL_NOT_FOUND
                        || pullResponse.getCode() == com.mq.proxy.core.protocol.ResponseCode.PULL_RETRY_IMMEDIATELY);

        if (pullResponse.getCode() == RemotingSysResponseCode.SUCCESS) {
            assertNotNull("Pull response extFields should not be null", pullResponse.getExtFields());
            assertNotNull("nextBeginOffset should not be null", pullResponse.getExtFields().get("nextBeginOffset"));
        }
    }

    private int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }
}
