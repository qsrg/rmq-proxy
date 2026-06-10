package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class ProcessorRegisterTest {

    @Test
    public void shouldRegisterPullMessageOnDedicatedExecutor() throws Exception {
        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setPullExecutorThreadNums(24);
        NettyRemotingServer remotingServer = new NettyRemotingServer(serverConfig);

        ProcessorRegister.registerProcessors(
                remotingServer,
                mock(MessageEngine.class),
                mock(VirtualRouteManager.class),
                mock(ClientConnectionManager.class)
        );

        Object pullRegistration = processorTable(remotingServer).get(RequestCode.PULL_MESSAGE);
        Object sendRegistration = processorTable(remotingServer).get(RequestCode.SEND_MESSAGE);
        ExecutorService pullExecutor = (ExecutorService) field(NettyRemotingServer.class, "pullExecutor").get(remotingServer);

        assertSame(pullExecutor, field(pullRegistration.getClass(), "executor").get(pullRegistration));
        assertNull(field(sendRegistration.getClass(), "executor").get(sendRegistration));
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Integer, Object> processorTable(NettyRemotingServer server) throws Exception {
        return (ConcurrentHashMap<Integer, Object>) field(NettyRemotingServer.class, "processorTable").get(server);
    }

    private static Field field(Class<?> type, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
