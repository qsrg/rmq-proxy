package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class RocketMQIntegrationTest {

    private static final String NAMESRV_ADDR =
            System.getProperty("test.namesrvAddr", RocketMQIntegrationSupport.DEFAULT_NAMESRV_ADDR);
    private static final String TEST_TOPIC = "PROXY_INTEGRATION_TEST";
    private static final String PRODUCER_GROUP = "PID_PROXY_TEST";
    private static final String CONSUMER_GROUP = "CID_PROXY_TEST";

    private EmbeddedRocketMQProxy proxy;
    private NettyRemotingClient proxyClient;

    @Before
    public void setUp() throws Exception {
        proxy = new EmbeddedRocketMQProxy(NAMESRV_ADDR);
        proxy.start();

        proxyClient = new NettyRemotingClient(new NettyClientConfig());
        proxyClient.start();

        System.out.println("Proxy started on " + proxy.getProxyAddr() + ", forwarding to Broker " + proxy.getBrokerAddr());
    }

    @After
    public void tearDown() {
        if (proxyClient != null) {
            proxyClient.shutdown();
        }
        if (proxy != null) {
            proxy.shutdown();
        }
    }

    @Test
    public void testProxySendMessage() throws Exception {
        String proxyAddr = proxy.getProxyAddr();

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", PRODUCER_GROUP);
        extFields.put("b", TEST_TOPIC);
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
        request.setBody("Hello Proxy Integration Test".getBytes("UTF-8"));

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Proxy send: code=" + response.getCode() + ", remark=" + response.getRemark() + ", extFields=" + response.getExtFields());

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull("msgId should not be null", response.getExtFields().get("msgId"));
        assertNotNull("queueId should not be null", response.getExtFields().get("queueId"));
        assertNotNull("queueOffset should not be null", response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testProxyQueryConsumerOffset() throws Exception {
        String proxyAddr = proxy.getProxyAddr();

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", CONSUMER_GROUP);
        extFields.put("topic", TEST_TOPIC);
        extFields.put("queueId", "0");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, null);
        request.setExtFields(extFields);

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Query offset: code=" + response.getCode() + ", extFields=" + response.getExtFields());

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            assertNotNull("offset should not be null", response.getExtFields().get("offset"));
        }
    }

    @Test
    public void testProxyUpdateConsumerOffset() throws Exception {
        String proxyAddr = proxy.getProxyAddr();

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", CONSUMER_GROUP);
        extFields.put("topic", TEST_TOPIC);
        extFields.put("queueId", "0");
        extFields.put("commitOffset", "0");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, null);
        request.setExtFields(extFields);

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Update offset: code=" + response.getCode());
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProxyGetRouteInfo() throws Exception {
        String proxyAddr = proxy.getProxyAddr();

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", TEST_TOPIC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setExtFields(extFields);

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Get route info: code=" + response.getCode() + ", bodyLen=" + (response.getBody() != null ? response.getBody().length : 0));

        assertTrue("Get route info should return SUCCESS or TOPIC_NOT_EXIST",
                response.getCode() == RemotingSysResponseCode.SUCCESS
                        || response.getCode() == ResponseCode.TOPIC_NOT_EXIST);

        if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
            String json = new String(response.getBody(), "UTF-8");
            assertTrue("Route info should contain brokerDatas", json.contains("brokerDatas"));
        }
    }
}
