package com.mq.proxy.core;

import com.mq.proxy.core.config.ProxyConfig;
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
}
