package com.mq.proxy.core;

import com.mq.proxy.core.config.ProxyConfig;
import com.mq.proxy.core.server.NettyServerConfig;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ProxyStartupTest {

    @Test
    public void shouldFailWhenNamesrvAddrIsBlank() throws Exception {
        ProxyConfig config = new ProxyConfig();
        config.setNamesrvAddr("  ");

        Method method = ProxyStartup.class.getDeclaredMethod("createStorageAdapter", ProxyConfig.class);
        method.setAccessible(true);

        try {
            method.invoke(null, config);
            fail("Expected createStorageAdapter to reject blank namesrvAddr");
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            assertEquals(IllegalArgumentException.class, cause.getClass());
            assertEquals("proxy.namesrvAddr must not be blank", cause.getMessage());
        }
    }

    @Test
    public void shouldCreateVipServerConfigOnListenPortMinusTwo() throws Exception {
        ProxyConfig config = new ProxyConfig();
        config.setListenPort(19876);
        config.setBossThreadNums(2);
        config.setWorkerThreadNums(3);
        config.setRequestProcessorThreadNums(4);
        config.setPullExecutorThreadNums(5);

        Method method = ProxyStartup.class.getDeclaredMethod("createVipServerConfig", ProxyConfig.class);
        method.setAccessible(true);

        NettyServerConfig vipConfig = (NettyServerConfig) method.invoke(null, config);

        assertEquals(19874, vipConfig.getListenPort());
        assertEquals(2, vipConfig.getBossThreadNums());
        assertEquals(3, vipConfig.getWorkerThreadNums());
        assertEquals(4, vipConfig.getRequestProcessorThreadNums());
        assertEquals(5, vipConfig.getPullExecutorThreadNums());
    }
}
