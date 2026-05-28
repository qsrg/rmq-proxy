package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class NativeTlsIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "NATIVE_TLS_TEST";
    private static final String PRODUCER_GROUP = "PID_NATIVE_TLS_TEST";
    private static final String CONSUMER_GROUP = "CID_NATIVE_TLS_TEST";

    private static final String KEYSTORE_PATH = "/tmp/proxy-tls-native-keystore.jks";
    private static final String CERT_PATH = "/tmp/proxy-tls-native-cert.cer";
    private static final String TRUSTSTORE_PATH = "/tmp/proxy-tls-native-truststore.jks";
    private static final String STORE_PASS = "testpass";

    private NettyRemotingClient namesrvClient;
    private String brokerAddr;
    private NettyRemotingServer proxyServer;
    private NettyRemotingClient tlsClient;
    private int proxyPort;
    private StorageAdapterManager storageAdapterManager;
    private MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;

    @Before
    public void setUp() throws Exception {
        generateCertificates();

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
        serverConfig.setTlsEnabled(true);
        serverConfig.setTlsKeyStorePath(KEYSTORE_PATH);
        serverConfig.setTlsKeyStorePassword(STORE_PASS);
        serverConfig.setTlsKeyStoreType("JKS");
        proxyServer = new NettyRemotingServer(serverConfig);
        proxyServer.setClientConnectionManager(clientConnectionManager);
        ProcessorRegister.registerProcessors(proxyServer, messageEngine, virtualRouteManager, clientConnectionManager);
        proxyServer.start();
        messageEngine.setRemotingServer(proxyServer);
        heartbeatService.start();

        NettyClientConfig tlsClientConfig = new NettyClientConfig();
        tlsClientConfig.setTlsEnabled(true);
        tlsClientConfig.setTlsTrustStorePath(TRUSTSTORE_PATH);
        tlsClientConfig.setTlsTrustStorePassword(STORE_PASS);
        tlsClientConfig.setTlsKeyStoreType("JKS");
        tlsClient = new NettyRemotingClient(tlsClientConfig);
        tlsClient.start();

        System.out.println("TLS Proxy started on 127.0.0.1:" + proxyPort);
    }

    @After
    public void tearDown() {
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (tlsClient != null) {
            tlsClient.shutdown();
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

        deleteFile(KEYSTORE_PATH);
        deleteFile(CERT_PATH);
        deleteFile(TRUSTSTORE_PATH);
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
        request.setBody("Hello TLS Proxy Test".getBytes("UTF-8"));

        RemotingCommand response = tlsClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("TLS proxy send: code=" + response.getCode() + ", remark=" + response.getRemark() + ", extFields=" + response.getExtFields());

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull("msgId should not be null", response.getExtFields().get("msgId"));
        assertNotNull("queueId should not be null", response.getExtFields().get("queueId"));
        assertNotNull("queueOffset should not be null", response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testTlsProxyPullMessage() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        // First send a message through TLS proxy
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
        sendRequest.setBody("TLS Pull Test Message".getBytes("UTF-8"));

        RemotingCommand sendResponse = tlsClient.invokeSync(proxyAddr, sendRequest, 10000);
        assertEquals("Send should succeed before pull test", RemotingSysResponseCode.SUCCESS, sendResponse.getCode());

        int queueId = Integer.parseInt(sendResponse.getExtFields().get("queueId"));
        long queueOffset = Long.parseLong(sendResponse.getExtFields().get("queueOffset"));

        Thread.sleep(500);

        // Then pull the message through TLS proxy
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

        RemotingCommand pullResponse = tlsClient.invokeSync(proxyAddr, pullRequest, 10000);
        System.out.println("TLS pull response: code=" + pullResponse.getCode() + ", bodyLen=" + (pullResponse.getBody() != null ? pullResponse.getBody().length : 0));

        assertTrue("Pull should return SUCCESS, PULL_NOT_FOUND, PULL_RETRY_IMMEDIATELY or SUBSCRIPTION_GROUP_NOT_EXIST",
                pullResponse.getCode() == RemotingSysResponseCode.SUCCESS
                        || pullResponse.getCode() == ResponseCode.PULL_NOT_FOUND
                        || pullResponse.getCode() == ResponseCode.PULL_RETRY_IMMEDIATELY
                        || pullResponse.getCode() == 24);
    }

    private void generateCertificates() throws Exception {
        // Generate self-signed JKS keystore
        ProcessBuilder pb1 = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", "proxy",
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "1",
                "-keystore", KEYSTORE_PATH,
                "-storepass", STORE_PASS,
                "-keypass", STORE_PASS,
                "-dname", "CN=proxy-test,OU=test,O=test,L=test,ST=test,C=CN",
                "-deststoretype", "JKS"
        );
        pb1.inheritIO();
        Process p1 = pb1.start();
        int exitCode1 = p1.waitFor();
        assertEquals("keytool genkeypair should succeed", 0, exitCode1);

        // Export certificate
        ProcessBuilder pb2 = new ProcessBuilder(
                "keytool", "-exportcert",
                "-alias", "proxy",
                "-keystore", KEYSTORE_PATH,
                "-storepass", STORE_PASS,
                "-file", CERT_PATH
        );
        pb2.inheritIO();
        Process p2 = pb2.start();
        int exitCode2 = p2.waitFor();
        assertEquals("keytool exportcert should succeed", 0, exitCode2);

        // Import certificate into truststore
        ProcessBuilder pb3 = new ProcessBuilder(
                "keytool", "-importcert",
                "-alias", "proxy",
                "-file", CERT_PATH,
                "-keystore", TRUSTSTORE_PATH,
                "-storepass", STORE_PASS,
                "-noprompt"
        );
        pb3.inheritIO();
        Process p3 = pb3.start();
        int exitCode3 = p3.waitFor();
        assertEquals("keytool importcert should succeed", 0, exitCode3);

        System.out.println("TLS certificates generated successfully");
    }

    private void deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            if (file.delete()) {
                System.out.println("Deleted: " + path);
            } else {
                System.out.println("Failed to delete: " + path);
            }
        }
    }

    private int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }
}
