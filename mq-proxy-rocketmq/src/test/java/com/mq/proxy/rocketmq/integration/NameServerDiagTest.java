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

public class NameServerDiagTest {

    private NettyRemotingClient client;

    @Before
    public void setUp() {
        client = new NettyRemotingClient(new NettyClientConfig());
        client.start();
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.shutdown();
        }
    }

    @Test
    public void testConnectToNameServer() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        System.out.println("Sending GET_BROKER_CLUSTER_INFO to 127.0.0.1:9876 ...");
        RemotingCommand response = client.invokeSync("127.0.0.1:9876", request, 5000);
        System.out.println("Response: code=" + response.getCode() + ", remark=" + response.getRemark()
                + ", bodyLen=" + (response.getBody() != null ? response.getBody().length : 0)
                + ", extFields=" + response.getExtFields()
                + ", serializeType=" + response.getSerializeTypeCurrentRPC());
        if (response.getBody() != null) {
            System.out.println("Body preview: " + new String(response.getBody(), "UTF-8").substring(0, Math.min(500, response.getBody().length)));
        }
    }

    @Test
    public void testGetRouteInfo() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TBW102");
        request.setExtFields(extFields);
        request.makeCustomHeaderToNet();
        System.out.println("Sending GET_ROUTEINFO_BY_TOPIC for TBW102 to 127.0.0.1:9876 ...");
        RemotingCommand response = client.invokeSync("127.0.0.1:9876", request, 5000);
        System.out.println("Response: code=" + response.getCode() + ", remark=" + response.getRemark()
                + ", bodyLen=" + (response.getBody() != null ? response.getBody().length : 0)
                + ", extFields=" + response.getExtFields()
                + ", serializeType=" + response.getSerializeTypeCurrentRPC());
        if (response.getBody() != null) {
            System.out.println("Body preview: " + new String(response.getBody(), "UTF-8").substring(0, Math.min(500, response.getBody().length)));
        }
    }
}
