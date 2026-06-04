package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class NativeClientIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TOPIC = "NATIVE_CLIENT_TEST";
    private static final String PRODUCER_GROUP = "PID_NATIVE_TEST";
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

}
