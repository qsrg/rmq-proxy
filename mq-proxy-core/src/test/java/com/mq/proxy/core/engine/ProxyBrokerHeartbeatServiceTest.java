package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.InvokeCallback;
import com.mq.proxy.core.storage.StorageAdapter;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ProxyBrokerHeartbeatServiceTest {

    @Test
    public void testCreatedHeartbeatClientsShareNettyRuntimeExecutors() throws Exception {
        ProxyBrokerHeartbeatService service = new ProxyBrokerHeartbeatService(
                new ClientConnectionManager(),
                mock(StorageAdapter.class),
                "127.0.0.1",
                10911,
                new TestVirtualRouteManager("broker-a:10911"));

        try {
            NettyRemotingClient first = service.createClientEntry("client-001");
            NettyRemotingClient second = service.createClientEntry("client-002");

            assertTrue(first != second);
            assertSame(eventLoopGroup(first), eventLoopGroup(second));
            assertSame(callbackExecutor(first), callbackExecutor(second));
            assertSame(responseTableScanExecutor(first), responseTableScanExecutor(second));
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testHeartbeatServiceUsesSuppliedBrokerClientTlsConfig() throws Exception {
        NettyClientConfig brokerClientConfig = new NettyClientConfig();
        brokerClientConfig.setTlsEnabled(true);
        brokerClientConfig.setTlsClientCertPath("/path/to/proxy-client.crt");
        brokerClientConfig.setTlsClientKeyPath("/path/to/proxy-client.key");

        ProxyBrokerHeartbeatService service = new ProxyBrokerHeartbeatService(
                new ClientConnectionManager(),
                mock(StorageAdapter.class),
                "127.0.0.1",
                10911,
                new TestVirtualRouteManager("broker-a:10911"),
                brokerClientConfig);

        try {
            Field field = ProxyBrokerHeartbeatService.class.getDeclaredField("nettyClientConfig");
            field.setAccessible(true);
            NettyClientConfig actualConfig = (NettyClientConfig) field.get(service);

            assertSame(brokerClientConfig, actualConfig);
            assertTrue(actualConfig.isTlsEnabled());
            assertEquals("/path/to/proxy-client.crt", actualConfig.getTlsClientCertPath());
            assertEquals("/path/to/proxy-client.key", actualConfig.getTlsClientKeyPath());
        } finally {
            service.shutdown();
        }
    }

    @Test
    public void testUnregisterClientFansOutAllProducerAndConsumerGroups() {
        ClientConnectionManager.ClientInfo clientInfo =
                new ClientConnectionManager.ClientInfo(mock(Channel.class), "client-001");
        clientInfo.addProducerGroup("producerA");
        clientInfo.addProducerGroup("producerB");
        clientInfo.addConsumerGroup("consumerA");
        clientInfo.addConsumerGroup("consumerB");

        RecordingNettyRemotingClient remotingClient = new RecordingNettyRemotingClient();
        TestVirtualRouteManager routeManager = new TestVirtualRouteManager("broker-a:10911");
        TestProxyBrokerHeartbeatService service =
                new TestProxyBrokerHeartbeatService(routeManager, remotingClient);

        service.unregisterClient(clientInfo);

        assertEquals(Arrays.asList("broker-a:10911", "broker-a:10911", "broker-a:10911", "broker-a:10911"),
                remotingClient.invokedAddrs);
        assertEquals(4, remotingClient.sentCommands.size());
        assertContainsOnly(remotingClient.sentCommands,
                request("client-001", "producerA", null),
                request("client-001", "producerB", null),
                request("client-001", null, "consumerA"),
                request("client-001", null, "consumerB"));
        assertTrue(remotingClient.shutdownCalled);
    }

    @Test
    public void testNotifyConsumerIdsChangedUsesOnewayForwarding() throws Exception {
        ClientConnectionManager manager = new ClientConnectionManager();
        Channel consumerChannel = mock(Channel.class);
        when(consumerChannel.isActive()).thenReturn(true);
        manager.registerConsumer(consumerChannel, "client-001", "group-a", null, null, null, null);

        TestVirtualRouteManager routeManager = new TestVirtualRouteManager("broker-a:10911");
        TestProxyBrokerHeartbeatService service =
                new TestProxyBrokerHeartbeatService(manager, routeManager, new RecordingNettyRemotingClient());
        RecordingRemotingServer remotingServer = new RecordingRemotingServer();
        service.setRemotingServer(remotingServer);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, null);
        request.setExtFields(new java.util.HashMap<String, String>());

        RemotingCommand response = service.newForwardProcessor("client-001", RequestCode.NOTIFY_CONSUMER_IDS_CHANGED)
                .processRequest(mock(Channel.class), request);

        assertNull(response);
        assertEquals(1, remotingServer.onewayCount);
        assertEquals(0, remotingServer.syncCount);
        assertEquals(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, remotingServer.lastRequest.get().getCode());
        assertTrue(remotingServer.lastRequest.get().isOnewayRPC());
    }

    @Test
    public void testCleanupStaleClientsKeepsSharedBrokerClient() {
        ClientConnectionManager.ClientInfo clientInfo =
                new ClientConnectionManager.ClientInfo(mock(Channel.class), "client-001");
        clientInfo.addConsumerGroup("consumerA");

        RecordingNettyRemotingClient remotingClient = new RecordingNettyRemotingClient();
        TestVirtualRouteManager routeManager = new TestVirtualRouteManager("broker-a:10911");
        TestProxyBrokerHeartbeatService service =
                new TestProxyBrokerHeartbeatService(routeManager, remotingClient);

        service.touchClient("client-001");
        service.touchSharedClient();
        service.cleanupWith(Collections.singletonList(clientInfo));

        assertTrue(service.clientPool().containsKey("client-001"));
        assertTrue(service.clientPool().containsKey("__proxy_shared_broker_client__"));
    }

    @Test
    public void testHeartbeatKeepsOriginalSubscriptionVersion() {
        ClientConnectionManager manager = new ClientConnectionManager();
        Channel consumerChannel = mock(Channel.class);
        when(consumerChannel.isActive()).thenReturn(true);

        com.mq.proxy.core.protocol.heartbeat.HeartbeatData.SubscriptionData sub =
                new com.mq.proxy.core.protocol.heartbeat.HeartbeatData.SubscriptionData();
        sub.setTopic("TopicA");
        sub.setSubString("TagA");
        sub.setSubVersion(123L);
        java.util.Set<com.mq.proxy.core.protocol.heartbeat.HeartbeatData.SubscriptionData> subscriptions =
                new java.util.HashSet<>();
        subscriptions.add(sub);

        manager.registerConsumer(consumerChannel, "client-001", "group-a",
                "CONSUME_PASSIVELY", "CLUSTERING", "CONSUME_FROM_LAST_OFFSET", subscriptions);

        RecordingNettyRemotingClient remotingClient = new RecordingNettyRemotingClient();
        remotingClient.channelActive = true;
        TestProxyBrokerHeartbeatService service =
                new TestProxyBrokerHeartbeatService(manager, new TestVirtualRouteManager("broker-a:10911"), remotingClient);

        service.sendHeartbeat();

        assertEquals(1, remotingClient.sentCommands.size());
        RemotingCommand request = remotingClient.sentCommands.get(0);
        com.mq.proxy.core.protocol.heartbeat.HeartbeatData heartbeatData =
                com.mq.proxy.core.protocol.heartbeat.HeartbeatData.decode(request.getBody());
        assertEquals(1, heartbeatData.getConsumerDataSet().size());

        com.mq.proxy.core.protocol.heartbeat.HeartbeatData.ConsumerData consumerData =
                heartbeatData.getConsumerDataSet().iterator().next();
        assertEquals("group-a", consumerData.getGroupName());
        assertEquals(1, consumerData.getSubscriptionDataSet().size());

        com.mq.proxy.core.protocol.heartbeat.HeartbeatData.SubscriptionData forwardedSub =
                consumerData.getSubscriptionDataSet().iterator().next();
        assertEquals(123L, forwardedSub.getSubVersion());
        assertEquals("TagA", forwardedSub.getSubString());
        assertNotNull(forwardedSub.getTagsSet());
        assertTrue(forwardedSub.getTagsSet().contains("TagA"));
    }

    @Test
    public void testHeartbeatUsesAsyncInvoke() {
        ClientConnectionManager manager = new ClientConnectionManager();
        Channel consumerChannel = mock(Channel.class);
        when(consumerChannel.isActive()).thenReturn(true);
        manager.registerConsumer(consumerChannel, "client-001", "group-a",
                "CONSUME_PASSIVELY", "CLUSTERING", "CONSUME_FROM_LAST_OFFSET", Collections.emptySet());

        RecordingNettyRemotingClient remotingClient = new RecordingNettyRemotingClient();
        remotingClient.channelActive = true;
        TestProxyBrokerHeartbeatService service =
                new TestProxyBrokerHeartbeatService(manager, new TestVirtualRouteManager("broker-a:10911"), remotingClient);

        service.sendHeartbeat();

        assertTrue(remotingClient.invokedAddrs.isEmpty());
        assertEquals(Collections.singletonList("broker-a:10911"), remotingClient.asyncInvokedAddrs);
        assertEquals(1, remotingClient.sentCommands.size());
    }

    private static void assertContainsOnly(List<RemotingCommand> actual, ExpectedRequest... expectedRequests) {
        List<ExpectedRequest> actualRequests = new ArrayList<>();
        for (RemotingCommand command : actual) {
            String clientId = command.getExtFields().get("clientID");
            String producerGroup = command.getExtFields().get("producerGroup");
            String consumerGroup = command.getExtFields().get("consumerGroup");
            actualRequests.add(new ExpectedRequest(clientId, producerGroup, consumerGroup));
        }
        Comparator<ExpectedRequest> comparator = Comparator
                .comparing(ExpectedRequest::getClientId)
                .thenComparing(ExpectedRequest::getProducerGroup, Comparator.nullsFirst(String::compareTo))
                .thenComparing(ExpectedRequest::getConsumerGroup, Comparator.nullsFirst(String::compareTo));
        Collections.sort(actualRequests, comparator);
        List<ExpectedRequest> expected = new ArrayList<>(Arrays.asList(expectedRequests));
        Collections.sort(expected, comparator);
        assertEquals(expected, actualRequests);
    }

    private static ExpectedRequest request(String clientId, String producerGroup, String consumerGroup) {
        return new ExpectedRequest(clientId, producerGroup, consumerGroup);
    }

    private static EventLoopGroup eventLoopGroup(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("eventLoopGroup");
        field.setAccessible(true);
        return (EventLoopGroup) field.get(client);
    }

    private static ExecutorService callbackExecutor(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("callbackExecutor");
        field.setAccessible(true);
        return (ExecutorService) field.get(client);
    }

    private static ScheduledExecutorService responseTableScanExecutor(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("responseTableScanExecutor");
        field.setAccessible(true);
        return (ScheduledExecutorService) field.get(client);
    }

    private static class TestProxyBrokerHeartbeatService extends ProxyBrokerHeartbeatService {
        private final NettyRemotingClient remotingClient;

        TestProxyBrokerHeartbeatService(VirtualRouteManager virtualRouteManager, NettyRemotingClient remotingClient) {
            this(new ClientConnectionManager(), virtualRouteManager, remotingClient);
        }

        TestProxyBrokerHeartbeatService(ClientConnectionManager manager,
                                        VirtualRouteManager virtualRouteManager,
                                        NettyRemotingClient remotingClient) {
            super(manager, mock(StorageAdapter.class), "127.0.0.1", 10911, virtualRouteManager);
            this.remotingClient = remotingClient;
        }

        @Override
        protected NettyRemotingClient createClient(String clientId) {
            return remotingClient;
        }

        com.mq.proxy.core.server.RemotingProcessor newForwardProcessor(String clientId, int requestCode) {
            return createBrokerToClientForwardProcessor(clientId, requestCode);
        }

        void touchClient(String clientId) {
            createClientEntry(clientId);
        }

        void touchSharedClient() {
            getSharedClientEntry();
        }

        void cleanupWith(List<ClientConnectionManager.ClientInfo> clientInfos) {
            cleanupClientEntries(clientInfos);
        }

        Map<String, NettyRemotingClient> clientPool() {
            return getClientChannelPool();
        }
    }

    private static class TestVirtualRouteManager extends VirtualRouteManager {
        private final List<String> brokerAddrs;

        TestVirtualRouteManager(String... brokerAddrs) {
            this.brokerAddrs = Arrays.asList(brokerAddrs);
        }

        @Override
        public List<String> getAllRealBrokerAddrs() {
            return brokerAddrs;
        }
    }

    private static class RecordingNettyRemotingClient extends NettyRemotingClient {
        private final List<String> invokedAddrs = new ArrayList<>();
        private final List<String> asyncInvokedAddrs = new ArrayList<>();
        private final List<RemotingCommand> sentCommands = new ArrayList<>();
        private boolean shutdownCalled;
        private boolean channelActive;

        RecordingNettyRemotingClient() {
            super(new com.mq.proxy.core.server.NettyClientConfig());
        }

        @Override
        public void start() {
        }

        @Override
        public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) {
            invokedAddrs.add(addr);
            sentCommands.add(request);
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK");
        }

        @Override
        public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
            asyncInvokedAddrs.add(addr);
            sentCommands.add(request);
            invokeCallback.operationSucceed(RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK"));
        }

        @Override
        public Channel getAndCreateChannel(String addr) {
            Channel channel = mock(Channel.class);
            when(channel.isActive()).thenReturn(channelActive);
            return channel;
        }

        @Override
        public void shutdown() {
            shutdownCalled = true;
        }
    }

    private static class RecordingRemotingServer extends NettyRemotingServer {
        private int syncCount;
        private int onewayCount;
        private final AtomicReference<RemotingCommand> lastRequest = new AtomicReference<>();

        RecordingRemotingServer() {
            super(new com.mq.proxy.core.server.NettyServerConfig());
        }

        @Override
        public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) {
            syncCount++;
            lastRequest.set(request);
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK");
        }

        @Override
        public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) {
            request.markOnewayRPC();
            onewayCount++;
            lastRequest.set(request);
        }
    }

    private static class ExpectedRequest {
        private final String clientId;
        private final String producerGroup;
        private final String consumerGroup;

        ExpectedRequest(String clientId, String producerGroup, String consumerGroup) {
            this.clientId = clientId;
            this.producerGroup = producerGroup;
            this.consumerGroup = consumerGroup;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ExpectedRequest)) {
                return false;
            }
            ExpectedRequest that = (ExpectedRequest) o;
            return java.util.Objects.equals(clientId, that.clientId)
                    && java.util.Objects.equals(producerGroup, that.producerGroup)
                    && java.util.Objects.equals(consumerGroup, that.consumerGroup);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(clientId, producerGroup, consumerGroup);
        }

        String getClientId() {
            return clientId;
        }

        String getProducerGroup() {
            return producerGroup;
        }

        String getConsumerGroup() {
            return consumerGroup;
        }
    }
}
