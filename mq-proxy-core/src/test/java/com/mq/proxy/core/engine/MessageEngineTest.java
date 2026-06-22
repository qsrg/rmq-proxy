package com.mq.proxy.core.engine;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.storage.PullMessageCallback;
import com.mq.proxy.core.storage.PutMessageCallback;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
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

    @Test
    public void testPullMessageAsyncFailureReturnsSystemErrorWithPreservedOffset() throws Exception {
        StorageAdapter adapter = mock(StorageAdapter.class);
        VirtualRouteManager routeManager = mock(VirtualRouteManager.class);
        MessageEngine messageEngine = new MessageEngine(adapter);
        messageEngine.setVirtualRouteManager(routeManager);

        when(routeManager.getRealBrokerAddr("broker-a")).thenReturn("127.0.0.1:10911");
        doAnswer(invocation -> {
            PullMessageCallback callback = invocation.getArgument(12);
            callback.onException(new RuntimeException("invokeAsync timeout"));
            return null;
        }).when(adapter).pullMessageAsync(eq("group-a"), eq("TopicA"), eq(0), eq(5L), eq(32),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), eq("127.0.0.1:10911"), any(PullMessageCallback.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PullResult> callbackResult = new AtomicReference<>();

        messageEngine.pullMessageAsync("group-a", "TopicA", 0, 5L, 32,
                3, 5L, 15000L, "TagA", "TAG", 123L, "broker-a", new PullMessageCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                        callbackResult.set(pullResult);
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        fail("MessageEngine should normalize async pull failures to PullResult");
                    }
                });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(5L, callbackResult.get().getNextBeginOffset());
        assertEquals(0L, callbackResult.get().getMinOffset());
        assertEquals(5L, callbackResult.get().getMaxOffset());
        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, callbackResult.get().getResponseCode());
    }

    @Test
    public void testPutMessageAsyncCompletesCallbackOnlyOnceWhenAdapterCallbacksThenThrows() {
        StorageAdapter adapter = mock(StorageAdapter.class);
        VirtualRouteManager routeManager = mock(VirtualRouteManager.class);
        MessageEngine messageEngine = new MessageEngine(adapter);
        messageEngine.setVirtualRouteManager(routeManager);
        InternalMessage message = new InternalMessage();
        message.setBrokerName("broker-a");
        message.setTopic("TopicA");
        when(routeManager.getRealBrokerAddr("broker-a")).thenReturn("127.0.0.1:10911");
        doAnswer(invocation -> {
            PutMessageCallback callback = invocation.getArgument(2);
            callback.onSuccess(PutResult.success("msg-1", 0, 1L));
            throw new RuntimeException("late invokeAsync exception");
        }).when(adapter).putMessageAsync(eq(message), eq("127.0.0.1:10911"), any(PutMessageCallback.class));
        AtomicInteger callbackCount = new AtomicInteger();

        messageEngine.putMessageAsync(message, new PutMessageCallback() {
            @Override
            public void onSuccess(PutResult putResult) {
                callbackCount.incrementAndGet();
            }

            @Override
            public void onException(Throwable throwable) {
                callbackCount.incrementAndGet();
            }
        });

        assertEquals(1, callbackCount.get());
    }
}
