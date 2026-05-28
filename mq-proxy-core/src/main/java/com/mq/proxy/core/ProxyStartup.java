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
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

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

        log.info("Proxy starting with config: listenPort={}, proxyHost={}, namesrvAddr={}, storageAdapterType={}, registerProxyToNameServer={}",
                proxyConfig.getListenPort(), proxyConfig.getProxyHost(), proxyConfig.getNamesrvAddr(),
                proxyConfig.getStorageAdapterType(), proxyConfig.isRegisterProxyToNameServer());

        StorageAdapterManager storageAdapterManager = new StorageAdapterManager();
        try {
            Map<String, StorageConfig> configs = new HashMap<>();
            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setAdapterType(proxyConfig.getStorageAdapterType());
            storageConfig.setNamesrvAddr(proxyConfig.getNamesrvAddr());
            storageConfig.setBrokerAddr(proxyConfig.getBrokerAddr());
            storageConfig.setConnectTimeoutMillis(proxyConfig.getConnectTimeoutMillis());
            configs.put(proxyConfig.getStorageAdapterType(), storageConfig);
            storageAdapterManager.initializeAll(configs);

            for (Map.Entry<String, String> entry : proxyConfig.getTopicRouteConfig().entrySet()) {
                storageAdapterManager.routeTopic(entry.getKey(), entry.getValue());
            }
        } catch (Exception e) {
            log.error("Failed to initialize StorageAdapterManager", e);
            System.exit(1);
        }

        MessageEngine messageEngine = new MessageEngine(storageAdapterManager);

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
                clientConnectionManager, storageAdapterManager, proxyConfig.getProxyHost(), proxyConfig.getListenPort());

        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(proxyConfig.getListenPort());
        nettyServerConfig.setBossThreadNums(proxyConfig.getBossThreadNums());
        nettyServerConfig.setWorkerThreadNums(proxyConfig.getWorkerThreadNums());
        nettyServerConfig.setTlsEnabled(proxyConfig.isTlsEnabled());
        nettyServerConfig.setTlsKeyStorePath(proxyConfig.getTlsKeyStorePath());
        nettyServerConfig.setTlsKeyStorePassword(proxyConfig.getTlsKeyStorePassword());
        nettyServerConfig.setTlsTrustStorePath(proxyConfig.getTlsTrustStorePath());
        nettyServerConfig.setTlsTrustStorePassword(proxyConfig.getTlsTrustStorePassword());
        nettyServerConfig.setTlsKeyStoreType(proxyConfig.getTlsKeyStoreType());
        nettyServerConfig.setTlsClientAuth(proxyConfig.isTlsClientAuth());

        NettyRemotingServer remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(remotingServer);
        ProcessorRegister.registerProcessors(remotingServer, messageEngine, virtualRouteManager, clientConnectionManager);

        remotingServer.start();
        heartbeatService.start();

        final ProxyBrokerHeartbeatService finalHeartbeatService = heartbeatService;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down proxy...");
                finalHeartbeatService.shutdown();
                remotingServer.shutdown();
                virtualRouteManager.shutdown();
                storageAdapterManager.shutdownAll();
                log.info("Proxy shutdown complete");
            }
        }));

        log.info("Proxy started successfully on {}:{}", proxyConfig.getProxyHost(), proxyConfig.getListenPort());
    }
}
