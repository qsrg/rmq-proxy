package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RequestCode;
import io.netty.channel.Channel;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.assertSame;

public class NettyRemotingServerTest {

    @Test
    public void shouldShareRemotingStateWithCompanionServer() throws Exception {
        NettyServerConfig primaryConfig = new NettyServerConfig();
        primaryConfig.setListenPort(19876);
        NettyRemotingServer primary = new NettyRemotingServer(primaryConfig);
        primary.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, new RemotingProcessor() {
            @Override
            public RemotingCommand processRequest(Channel channel, RemotingCommand request) {
                return RemotingCommand.createResponseCommand(0);
            }
        });

        NettyServerConfig vipConfig = new NettyServerConfig();
        vipConfig.setListenPort(19874);
        NettyRemotingServer vip = new NettyRemotingServer(vipConfig, primary);

        assertSame(field(primary, "processorTable"), field(vip, "processorTable"));
        assertSame(field(primary, "responseTable"), field(vip, "responseTable"));
        assertSame(field(primary, "opaqueCounter"), field(vip, "opaqueCounter"));
        assertSame(field(primary, "pullExecutor"), field(vip, "pullExecutor"));
    }

    private static Object field(Object target, String name) throws Exception {
        Field field = NettyRemotingServer.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
