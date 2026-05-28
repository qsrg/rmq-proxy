package com.mq.proxy.core;

import com.mq.proxy.core.config.ProxyConfig;
import com.mq.proxy.core.config.ProxyConfigLoader;
import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyStartup {

    private static final Logger log = LoggerFactory.getLogger(ProxyStartup.class);

    public static void main(String[] args) {
        String configFilePath = null;
        for (int i = 0; i < args.length; i++) {
            if ("-c".equals(args[i]) && i + 1 < args.length) {
                configFilePath = args[i + 1];
                break;
            }
        }

        ProxyConfig proxyConfig;
        if (configFilePath != null) {
            proxyConfig = ProxyConfigLoader.loadFromFile(configFilePath);
        } else {
            proxyConfig = ProxyConfigLoader.loadDefault();
        }

        log.info("Proxy starting with config: listenPort={}, proxyHost={}, namesrvAddr={}, brokerAddr={}, registerProxyToNameServer={}",
                proxyConfig.getListenPort(), proxyConfig.getProxyHost(), proxyConfig.getNamesrvAddr(),
                proxyConfig.getBrokerAddr(), proxyConfig.isRegisterProxyToNameServer());

        StorageAdapter storageAdapter = createStorageAdapter(proxyConfig);

        MessageEngine messageEngine = new MessageEngine(storageAdapter);

        VirtualRouteManager virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.setProxyBrokerName(proxyConfig.getProxyBrokerName());
        virtualRouteManager.setProxyClusterName(proxyConfig.getProxyClusterName());
        virtualRouteManager.setRouteCacheExpireMillis(proxyConfig.getRouteCacheExpireMillis());
        virtualRouteManager.start(proxyConfig.getNamesrvAddr(), proxyConfig.getProxyHost(), proxyConfig.getListenPort());

        if (proxyConfig.isRegisterProxyToNameServer()) {
            boolean registered = virtualRouteManager.registerProxyToNameServer();
            log.info("Register proxy to NameServer result: {}", registered);
        }

        messageEngine.setVirtualRouteManager(virtualRouteManager);

        ClientConnectionManager clientConnectionManager = new ClientConnectionManager();

        ProxyBrokerHeartbeatService heartbeatService = new ProxyBrokerHeartbeatService(
                clientConnectionManager, storageAdapter, proxyConfig.getProxyHost(), proxyConfig.getListenPort());

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(proxyConfig.getListenPort());
        nettyServerConfig.setBossThreadNums(proxyConfig.getBossThreadNums());
        nettyServerConfig.setWorkerThreadNums(proxyConfig.getWorkerThreadNums());

        if (proxyConfig.isTlsEnabled()) {
            nettyServerConfig.setTlsEnabled(true);
            nettyServerConfig.setTlsCertPath(proxyConfig.getTlsCertPath());
            nettyServerConfig.setTlsKeyPath(proxyConfig.getTlsKeyPath());
            nettyServerConfig.setTlsTrustCertPath(proxyConfig.getTlsTrustCertPath());
            nettyServerConfig.setTlsClientAuth(proxyConfig.isTlsClientAuth());
            log.info("TLS enabled: certPath={}, keyPath={}, clientAuth={}",
                    proxyConfig.getTlsCertPath(), proxyConfig.getTlsKeyPath(), proxyConfig.isTlsClientAuth());
        }

        NettyRemotingServer remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(remotingServer);
        ProcessorRegister.registerProcessors(remotingServer, messageEngine, virtualRouteManager, clientConnectionManager);

        remotingServer.start();
        heartbeatService.start();

        final ProxyBrokerHeartbeatService finalHeartbeatService = heartbeatService;
        final StorageAdapter finalStorageAdapter = storageAdapter;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down proxy...");
                finalHeartbeatService.shutdown();
                remotingServer.shutdown();
                virtualRouteManager.shutdown();
                finalStorageAdapter.shutdown();
                log.info("Proxy shutdown complete");
            }
        }));

        log.info("Proxy started successfully on {}:{}", proxyConfig.getProxyHost(), proxyConfig.getListenPort());
    }

    private static StorageAdapter createStorageAdapter(ProxyConfig proxyConfig) {
        String adapterClassName;
        if (proxyConfig.getBrokerAddr() != null && !proxyConfig.getBrokerAddr().isEmpty()) {
            adapterClassName = "com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter";
        } else {
            adapterClassName = "com.mq.proxy.mock.adapter.MockStorageAdapter";
            log.info("No brokerAddr configured, using MockStorageAdapter");
        }

        try {
            Class<?> clazz = Class.forName(adapterClassName);
            StorageAdapter adapter = (StorageAdapter) clazz.newInstance();

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setNamesrvAddr(proxyConfig.getNamesrvAddr());
            storageConfig.setBrokerAddr(proxyConfig.getBrokerAddr());
            storageConfig.setConnectTimeoutMillis(proxyConfig.getConnectTimeoutMillis());
            adapter.initialize(storageConfig);

            return adapter;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create StorageAdapter: " + adapterClassName, e);
        }
    }
}
