package com.mq.proxy.core.engine;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.model.PullResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageEngineTest {

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

    @Before
    public void setUp() {
        logger = (Logger) LoggerFactory.getLogger(MessageEngine.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
    }

    @After
    public void tearDown() {
        logger.detachAppender(appender);
        appender.stop();
    }

    @Test
    public void testPullMessageFailureReturnsSystemErrorWithPreservedOffset() throws Exception {
        StorageAdapter adapter = mock(StorageAdapter.class);
        VirtualRouteManager routeManager = mock(VirtualRouteManager.class);
        MessageEngine messageEngine = new MessageEngine(adapter);
        messageEngine.setVirtualRouteManager(routeManager);

        when(routeManager.getRealBrokerAddr("broker-a")).thenReturn("127.0.0.1:10911");
        when(adapter.pullMessage(eq("group-a"), eq("TopicA"), eq(0), eq(5L), eq(32),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), eq("127.0.0.1:10911")))
                .thenThrow(new RuntimeException("invokeSync timeout"));

        PullResult result = messageEngine.pullMessage("group-a", "TopicA", 0, 5L, 32,
                3, 5L, 15000L, "TagA", "TAG", 123L, "broker-a");

        assertEquals(5L, result.getNextBeginOffset());
        assertEquals(0L, result.getMinOffset());
        assertEquals(5L, result.getMaxOffset());
        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, result.getResponseCode());
        assertEquals(1, appender.list.size());

        ILoggingEvent event = appender.list.get(0);
        assertTrue(event.getFormattedMessage().contains("brokerAddr=127.0.0.1:10911"));
        assertTrue(event.getFormattedMessage().contains("preservedNextBeginOffset=5"));
        assertNotNull(event.getThrowableProxy());
        assertEquals("java.lang.RuntimeException", event.getThrowableProxy().getClassName());
    }
}
