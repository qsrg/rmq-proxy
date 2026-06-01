package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.UpstreamConsumerSessionManager;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter;

class EmbeddedRocketMQProxy {

    private final String namesrvAddr;
    private final String proxyHost;

    private NettyRemotingClient namesrvClient;
    private String brokerAddr;
    private NettyRemotingServer proxyServer;
    private int proxyPort;
    private StorageAdapter storageAdapter;
    private MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;
    private boolean started;

    EmbeddedRocketMQProxy(String namesrvAddr) {
        this(namesrvAddr, "127.0.0.1");
    }

    EmbeddedRocketMQProxy(String namesrvAddr, String proxyHost) {
        this.namesrvAddr = namesrvAddr;
        this.proxyHost = proxyHost;
    }

    void start() throws Exception {
        if (started) {
            return;
        }

        namesrvClient = new NettyRemotingClient(new NettyClientConfig());
        namesrvClient.start();

        brokerAddr = RocketMQIntegrationSupport.discoverBrokerAddr(namesrvClient, namesrvAddr);
        if (brokerAddr == null) {
            throw new IllegalStateException("Broker not found, is RocketMQ running on " + namesrvAddr + "?");
        }

        proxyPort = RocketMQIntegrationSupport.findAvailablePort();

        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setNamesrvAddr(namesrvAddr);
        storageConfig.setConnectTimeoutMillis(5000);

        storageAdapter = new RocketMQStorageAdapter();
        storageAdapter.initialize(storageConfig);

        messageEngine = new MessageEngine(storageAdapter);

        virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.start(namesrvAddr, proxyHost, proxyPort);
        messageEngine.setVirtualRouteManager(virtualRouteManager);

        clientConnectionManager = new ClientConnectionManager();
        heartbeatService = new ProxyBrokerHeartbeatService(
                clientConnectionManager, storageAdapter, proxyHost, proxyPort, virtualRouteManager);

        messageEngine.setOnSubscriptionNotLatest(new Runnable() {
            @Override
            public void run() {
                heartbeatService.sendHeartbeat();
            }
        });

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(proxyPort);
        configureServer(serverConfig);
        proxyServer = new NettyRemotingServer(serverConfig);
        proxyServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(proxyServer);
        heartbeatService.setRemotingServer(proxyServer);
        clientConnectionManager.addClientInactiveListener(clientInfo -> heartbeatService.unregisterClient(clientInfo));
        ProcessorRegister.registerProcessors(proxyServer, messageEngine, virtualRouteManager,
                clientConnectionManager, (UpstreamConsumerSessionManager) heartbeatService);

        proxyServer.start();
        heartbeatService.start();
        started = true;
    }

    protected void configureServer(NettyServerConfig serverConfig) {
    }

    void shutdown() {
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (proxyServer != null) {
            proxyServer.shutdown();
        }
        if (virtualRouteManager != null) {
            virtualRouteManager.shutdown();
        }
        if (storageAdapter != null) {
            storageAdapter.shutdown();
        }
        if (namesrvClient != null) {
            namesrvClient.shutdown();
        }
        started = false;
    }

    String getBrokerAddr() {
        return brokerAddr;
    }

    String getProxyAddr() {
        return proxyHost + ":" + proxyPort;
    }

    NettyRemotingClient getNamesrvClient() {
        return namesrvClient;
    }
}
