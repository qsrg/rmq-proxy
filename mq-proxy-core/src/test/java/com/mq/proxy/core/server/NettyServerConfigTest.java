package com.mq.proxy.core.server;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NettyServerConfigTest {

    @Test
    public void shouldConfigureRequestProcessorThreadNums() {
        NettyServerConfig config = new NettyServerConfig();

        config.setRequestProcessorThreadNums(64);

        assertEquals(64, config.getRequestProcessorThreadNums());
    }
}
