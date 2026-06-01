package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class NativeClientTlsIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "TLS_NATIVE_CLIENT_TEST";
    private static final String PRODUCER_GROUP = "PID_TLS_NATIVE_TEST";
    private static final String CONSUMER_GROUP = "CID_TLS_NATIVE_TEST";

    private static final String CERT_PATH = "/tmp/proxy-tls-server.crt";
    private static final String KEY_PATH = "/tmp/proxy-tls-server.key";

    private EmbeddedTlsRocketMQProxy proxy;

    @Before
    public void setUp() throws Exception {
        proxy = new EmbeddedTlsRocketMQProxy(NAMESRV_ADDR, CERT_PATH, KEY_PATH);
        proxy.start();
        System.out.println("TLS Proxy started on " + proxy.getProxyAddr() + ", broker=" + proxy.getBrokerAddr());
    }

    @After
    public void tearDown() {
        if (proxy != null) {
            proxy.shutdown();
        }
    }

    @Test
    public void testTlsProxyRouteInfo() throws Exception {
        String proxyAddr = proxy.getProxyAddr();

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", TOPIC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setExtFields(extFields);

        System.out.println("Sending TLS route request to proxy at " + proxyAddr);
        RemotingCommand response = proxy.getTlsClient().invokeSync(proxyAddr, request, 5000);

        System.out.println("Route response code: " + response.getCode());
        System.out.println("Route response body: " + (response.getBody() != null ?
                new String(response.getBody(), java.nio.charset.StandardCharsets.UTF_8) : "null"));

        assertEquals("Route response should be SUCCESS",
                RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull("Route response body should not be null", response.getBody());
    }

    @Test
    public void testTlsProxySendMessage() throws Exception {
        String proxyAddr = proxy.getProxyAddr();

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
        RemotingCommand response = proxy.getTlsClient().invokeSync(proxyAddr, request, 10000);

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
        String proxyAddr = proxy.getProxyAddr();

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

        RemotingCommand sendResponse = proxy.getTlsClient().invokeSync(proxyAddr, sendRequest, 10000);
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
        RemotingCommand pullResponse = proxy.getTlsClient().invokeSync(proxyAddr, pullRequest, 10000);

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
}
