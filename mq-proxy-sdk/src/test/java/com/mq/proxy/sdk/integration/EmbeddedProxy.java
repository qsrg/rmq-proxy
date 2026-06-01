package com.mq.proxy.sdk.integration;

import com.mq.proxy.core.config.ProxyConfig;
import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.UpstreamConsumerSessionManager;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedProxy {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedProxy.class);

    private final ProxyConfig proxyConfig;
    private NettyRemotingServer remotingServer;
    private VirtualRouteManager virtualRouteManager;
    private StorageAdapter storageAdapter;
    private MessageEngine messageEngine;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;
    private volatile boolean started = false;

    public EmbeddedProxy() {
        this.proxyConfig = new ProxyConfig();
        this.proxyConfig.setListenPort(19876);
        this.proxyConfig.setProxyHost("127.0.0.1");
        this.proxyConfig.setNamesrvAddr("127.0.0.1:9876");
        this.proxyConfig.setWorkerThreadNums(2);
        this.proxyConfig.setBossThreadNums(1);
    }

    public void start() throws Exception {
        if (started) {
            return;
        }

        log.info("Starting embedded proxy on port {}...", proxyConfig.getListenPort());

        storageAdapter = new RocketMQStorageAdapter();
        StorageConfig storageConfig = new StorageConfig();
        storageConfig.setNamesrvAddr(proxyConfig.getNamesrvAddr());
        storageConfig.setConnectTimeoutMillis(proxyConfig.getConnectTimeoutMillis());
        storageAdapter.initialize(storageConfig);

        messageEngine = new MessageEngine(storageAdapter);

        virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.setRouteCacheExpireMillis(proxyConfig.getRouteCacheExpireMillis());
        virtualRouteManager.start(proxyConfig.getNamesrvAddr(), proxyConfig.getProxyHost(), proxyConfig.getListenPort());

        messageEngine.setVirtualRouteManager(virtualRouteManager);

        clientConnectionManager = new ClientConnectionManager();

        heartbeatService = new ProxyBrokerHeartbeatService(
                clientConnectionManager, storageAdapter, proxyConfig.getProxyHost(), proxyConfig.getListenPort(),
                virtualRouteManager);

        messageEngine.setOnSubscriptionNotLatest(() -> heartbeatService.sendHeartbeat());

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(proxyConfig.getListenPort());
        nettyServerConfig.setBossThreadNums(proxyConfig.getBossThreadNums());
        nettyServerConfig.setWorkerThreadNums(proxyConfig.getWorkerThreadNums());

        remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(remotingServer);
        heartbeatService.setRemotingServer(remotingServer);
        clientConnectionManager.addClientInactiveListener(heartbeatService::unregisterClient);
        ProcessorRegister.registerProcessors(remotingServer, messageEngine, virtualRouteManager,
                clientConnectionManager, (UpstreamConsumerSessionManager) heartbeatService);

        remotingServer.start();
        heartbeatService.start();

        started = true;
        log.info("Embedded proxy started successfully on {}:{}", proxyConfig.getProxyHost(), proxyConfig.getListenPort());
    }

    public void shutdown() {
        if (!started) {
            return;
        }
        log.info("Shutting down embedded proxy...");
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (remotingServer != null) {
            remotingServer.shutdown();
        }
        if (virtualRouteManager != null) {
            virtualRouteManager.shutdown();
        }
        if (storageAdapter != null) {
            storageAdapter.shutdown();
        }
        started = false;
        log.info("Embedded proxy shutdown complete");
    }

    public int getPort() {
        return proxyConfig.getListenPort();
    }

    public String getProxyAddr() {
        return proxyConfig.getProxyHost() + ":" + proxyConfig.getListenPort();
    }

    public boolean isStarted() {
        return started;
    }
}
