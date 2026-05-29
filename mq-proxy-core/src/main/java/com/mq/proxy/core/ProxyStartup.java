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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class ProxyStartup {

    private static final Logger log = LoggerFactory.getLogger(ProxyStartup.class);
    private static final String ROCKETMQ_STORAGE_ADAPTER = "com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter";

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

        log.info("Proxy starting with config: listenPort={}, proxyHost={}, namesrvAddr={}",
                proxyConfig.getListenPort(), proxyConfig.getProxyHost(), proxyConfig.getNamesrvAddr());

        if (proxyConfig.getProxyHost() == null || proxyConfig.getProxyHost().trim().isEmpty()) {
            String detectedHost = detectLocalHost();
            proxyConfig.setProxyHost(detectedHost);
            log.info("proxy.host not configured, auto-detected: {}", detectedHost);
        }

        StorageAdapter storageAdapter = createStorageAdapter(proxyConfig);

        MessageEngine messageEngine = new MessageEngine(storageAdapter);

        VirtualRouteManager virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.setRouteCacheExpireMillis(proxyConfig.getRouteCacheExpireMillis());
        virtualRouteManager.start(proxyConfig.getNamesrvAddr(), proxyConfig.getProxyHost(), proxyConfig.getListenPort());

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
        if (proxyConfig.getNamesrvAddr() == null || proxyConfig.getNamesrvAddr().trim().isEmpty()) {
            throw new IllegalArgumentException("proxy.namesrvAddr must not be blank");
        }

        try {
            Class<?> clazz = Class.forName(ROCKETMQ_STORAGE_ADAPTER);
            StorageAdapter adapter = (StorageAdapter) clazz.newInstance();

            StorageConfig storageConfig = new StorageConfig();
            storageConfig.setNamesrvAddr(proxyConfig.getNamesrvAddr());
            storageConfig.setConnectTimeoutMillis(proxyConfig.getConnectTimeoutMillis());
            adapter.initialize(storageConfig);

            return adapter;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create StorageAdapter: " + ROCKETMQ_STORAGE_ADAPTER, e);
        }
    }

    private static String detectLocalHost() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    NetworkInterface ni = interfaces.nextElement();
                    if (ni.isLoopback() || ni.isVirtual() || !ni.isUp()) {
                        continue;
                    }
                    Enumeration<InetAddress> addresses = ni.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress addr = addresses.nextElement();
                        if (addr instanceof Inet6Address) {
                            continue;
                        }
                        if (!addr.isLoopbackAddress()) {
                            return addr.getHostAddress();
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to detect local host, fallback to 127.0.0.1: {}", e.getMessage());
        }
        return "127.0.0.1";
    }
}
