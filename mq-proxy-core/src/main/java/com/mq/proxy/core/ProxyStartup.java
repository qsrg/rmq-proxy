package com.mq.proxy.core;

import com.mq.proxy.core.config.ProxyConfig;
import com.mq.proxy.core.config.ProxyConfigLoader;
import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Properties;

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
            proxyConfig = loadFromClasspath();
        }

        overrideFromSystemProperties(proxyConfig);

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
                clientConnectionManager, storageAdapter, proxyConfig.getProxyHost(), proxyConfig.getListenPort(),
                virtualRouteManager, createBrokerClientConfig(proxyConfig));

        messageEngine.setOnSubscriptionNotLatest(() -> heartbeatService.sendHeartbeat());

        NettyServerConfig nettyServerConfig = createServerConfig(proxyConfig, proxyConfig.getListenPort());

        NettyRemotingServer remotingServer = new NettyRemotingServer(nettyServerConfig);
        remotingServer.setClientConnectionManager(clientConnectionManager);
        messageEngine.setRemotingServer(remotingServer);
        heartbeatService.setRemotingServer(remotingServer);
        clientConnectionManager.addClientInactiveListener(heartbeatService::unregisterClient);
        ProcessorRegister.registerProcessors(remotingServer, messageEngine, virtualRouteManager,
                clientConnectionManager, heartbeatService);

        NettyServerConfig vipServerConfig = createVipServerConfig(proxyConfig);
        NettyRemotingServer vipRemotingServer = new NettyRemotingServer(vipServerConfig, remotingServer);
        vipRemotingServer.setClientConnectionManager(clientConnectionManager);

        boolean remotingServerStarted = false;
        boolean vipRemotingServerStarted = false;
        try {
            remotingServer.start();
            remotingServerStarted = true;
            vipRemotingServer.start();
            vipRemotingServerStarted = true;
            heartbeatService.start();
        } catch (RuntimeException e) {
            if (vipRemotingServerStarted) {
                vipRemotingServer.shutdown();
            }
            if (remotingServerStarted) {
                remotingServer.shutdown();
            }
            virtualRouteManager.shutdown();
            storageAdapter.shutdown();
            throw e;
        }

        final ProxyBrokerHeartbeatService finalHeartbeatService = heartbeatService;
        final StorageAdapter finalStorageAdapter = storageAdapter;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Shutting down proxy...");
                finalHeartbeatService.shutdown();
                vipRemotingServer.shutdown();
                remotingServer.shutdown();
                virtualRouteManager.shutdown();
                finalStorageAdapter.shutdown();
                log.info("Proxy shutdown complete");
            }
        }));

        log.info("Proxy started successfully on {}:{}, vipPort={}",
                proxyConfig.getProxyHost(), proxyConfig.getListenPort(), vipServerConfig.getListenPort());
    }

    private static NettyServerConfig createVipServerConfig(ProxyConfig proxyConfig) {
        int vipPort = proxyConfig.getListenPort() - 2;
        if (vipPort <= 0) {
            throw new IllegalArgumentException("proxy.listenPort must be greater than 2 to support VIP port");
        }
        return createServerConfig(proxyConfig, vipPort);
    }

    private static NettyServerConfig createServerConfig(ProxyConfig proxyConfig, int listenPort) {
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(listenPort);
        nettyServerConfig.setBossThreadNums(proxyConfig.getBossThreadNums());
        nettyServerConfig.setWorkerThreadNums(proxyConfig.getWorkerThreadNums());
        nettyServerConfig.setRequestProcessorThreadNums(proxyConfig.getRequestProcessorThreadNums());
        nettyServerConfig.setPullExecutorThreadNums(proxyConfig.getPullExecutorThreadNums());

        if (proxyConfig.isTlsEnabled()) {
            nettyServerConfig.setTlsEnabled(true);
            nettyServerConfig.setTlsCertPath(proxyConfig.getTlsCertPath());
            nettyServerConfig.setTlsKeyPath(proxyConfig.getTlsKeyPath());
            nettyServerConfig.setTlsTrustCertPath(proxyConfig.getTlsTrustCertPath());
            nettyServerConfig.setTlsClientAuth(proxyConfig.isTlsClientAuth());
            nettyServerConfig.setTlsMode(proxyConfig.getTlsMode());
            log.info("TLS enabled: certPath={}, keyPath={}, clientAuth={}, tlsMode={}",
                    proxyConfig.getTlsCertPath(), proxyConfig.getTlsKeyPath(),
                    proxyConfig.isTlsClientAuth(), proxyConfig.getTlsMode());
        }

        return nettyServerConfig;
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
            storageConfig.setUpstreamClientAsyncSemaphoreValue(proxyConfig.getUpstreamClientAsyncSemaphoreValue());
            storageConfig.setUpstreamProducerAsyncSemaphoreValue(proxyConfig.getUpstreamProducerAsyncSemaphoreValue());
            storageConfig.setUpstreamPullAsyncSemaphoreValue(proxyConfig.getUpstreamPullAsyncSemaphoreValue());
            storageConfig.setUpstreamClientChannelPoolSize(proxyConfig.getUpstreamClientChannelPoolSize());
            storageConfig.setUpstreamClientKeepAliveIntervalSeconds(proxyConfig.getUpstreamClientKeepAliveIntervalSeconds());

            // 上游 TLS 配置（proxy -> broker），与下游 TLS 独立
            if (proxyConfig.isUpstreamTlsEnabled()) {
                storageConfig.setUpstreamTlsEnabled(true);
                storageConfig.setUpstreamTlsClientCertPath(proxyConfig.getUpstreamTlsClientCertPath());
                storageConfig.setUpstreamTlsClientKeyPath(proxyConfig.getUpstreamTlsClientKeyPath());
                log.info("Upstream TLS enabled: clientCertPath={}, clientKeyPath={}",
                        proxyConfig.getUpstreamTlsClientCertPath(),
                        proxyConfig.getUpstreamTlsClientKeyPath());
            }
            adapter.initialize(storageConfig);

            return adapter;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create StorageAdapter: " + ROCKETMQ_STORAGE_ADAPTER, e);
        }
    }

    private static NettyClientConfig createBrokerClientConfig(ProxyConfig proxyConfig) {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setConnectTimeoutMillis(proxyConfig.getConnectTimeoutMillis());
        clientConfig.setClientAsyncSemaphoreValue(proxyConfig.getUpstreamClientAsyncSemaphoreValue());
        clientConfig.setClientKeepAliveIntervalSeconds(proxyConfig.getUpstreamClientKeepAliveIntervalSeconds());
        clientConfig.setClientKeepAliveRequestCode(com.mq.proxy.core.protocol.RequestCode.CHECK_CLIENT_CONFIG);
        if (proxyConfig.isUpstreamTlsEnabled()) {
            clientConfig.setTlsEnabled(true);
            clientConfig.setTlsClientCertPath(proxyConfig.getUpstreamTlsClientCertPath());
            clientConfig.setTlsClientKeyPath(proxyConfig.getUpstreamTlsClientKeyPath());
        }
        return clientConfig;
    }

    private static ProxyConfig loadFromClasspath() {
        String externalConfig = System.getProperty("proxy.config.file");
        if (externalConfig != null) {
            File file = new File(externalConfig);
            if (file.exists()) {
                log.info("Loading config from system property proxy.config.file: {}", externalConfig);
                return ProxyConfigLoader.loadFromFile(externalConfig);
            }
        }

        try (InputStream is = ProxyStartup.class.getClassLoader().getResourceAsStream("proxy.properties")) {
            if (is != null) {
                log.info("Loading config from classpath: proxy.properties");
                Properties props = new Properties();
                props.load(is);
                return ProxyConfigLoader.loadFromProperties(props);
            }
        } catch (Exception e) {
            log.warn("Failed to load config from classpath: {}", e.getMessage());
        }

        log.info("No config file found, using defaults");
        return ProxyConfigLoader.loadDefault();
    }

    private static void overrideFromSystemProperties(ProxyConfig config) {
        String namesrvAddr = System.getProperty("proxy.namesrvAddr");
        if (namesrvAddr != null && !namesrvAddr.isEmpty()) {
            config.setNamesrvAddr(namesrvAddr);
            log.info("Override namesrvAddr from system property: {}", namesrvAddr);
        }
        String listenPort = System.getProperty("proxy.listenPort");
        if (listenPort != null && !listenPort.isEmpty()) {
            config.setListenPort(Integer.parseInt(listenPort));
            log.info("Override listenPort from system property: {}", listenPort);
        }
        String proxyHost = System.getProperty("proxy.host");
        if (proxyHost != null && !proxyHost.isEmpty()) {
            config.setProxyHost(proxyHost);
            log.info("Override proxyHost from system property: {}", proxyHost);
        }
        String requestProcessorThreadNums = System.getProperty("proxy.requestProcessorThreadNums");
        if (requestProcessorThreadNums != null && !requestProcessorThreadNums.isEmpty()) {
            config.setRequestProcessorThreadNums(Integer.parseInt(requestProcessorThreadNums));
            log.info("Override requestProcessorThreadNums from system property: {}", requestProcessorThreadNums);
        }
        String upstreamClientAsyncSemaphoreValue = System.getProperty("proxy.upstreamClientAsyncSemaphoreValue");
        if (upstreamClientAsyncSemaphoreValue != null && !upstreamClientAsyncSemaphoreValue.isEmpty()) {
            config.setUpstreamClientAsyncSemaphoreValue(Integer.parseInt(upstreamClientAsyncSemaphoreValue));
            log.info("Override upstreamClientAsyncSemaphoreValue from system property: {}",
                    upstreamClientAsyncSemaphoreValue);
        }
        String upstreamProducerAsyncSemaphoreValue = System.getProperty("proxy.upstreamProducerAsyncSemaphoreValue");
        if (upstreamProducerAsyncSemaphoreValue != null && !upstreamProducerAsyncSemaphoreValue.isEmpty()) {
            config.setUpstreamProducerAsyncSemaphoreValue(Integer.parseInt(upstreamProducerAsyncSemaphoreValue));
            log.info("Override upstreamProducerAsyncSemaphoreValue from system property: {}",
                    upstreamProducerAsyncSemaphoreValue);
        }
        String upstreamPullAsyncSemaphoreValue = System.getProperty("proxy.upstreamPullAsyncSemaphoreValue");
        if (upstreamPullAsyncSemaphoreValue != null && !upstreamPullAsyncSemaphoreValue.isEmpty()) {
            config.setUpstreamPullAsyncSemaphoreValue(Integer.parseInt(upstreamPullAsyncSemaphoreValue));
            log.info("Override upstreamPullAsyncSemaphoreValue from system property: {}",
                    upstreamPullAsyncSemaphoreValue);
        }
        String upstreamClientChannelPoolSize = System.getProperty("proxy.upstreamClientChannelPoolSize");
        if (upstreamClientChannelPoolSize != null && !upstreamClientChannelPoolSize.isEmpty()) {
            config.setUpstreamClientChannelPoolSize(Integer.parseInt(upstreamClientChannelPoolSize));
            log.info("Override upstreamClientChannelPoolSize from system property: {}", upstreamClientChannelPoolSize);
        }
        String upstreamClientKeepAliveIntervalSeconds =
                System.getProperty("proxy.upstreamClientKeepAliveIntervalSeconds");
        if (upstreamClientKeepAliveIntervalSeconds != null && !upstreamClientKeepAliveIntervalSeconds.isEmpty()) {
            config.setUpstreamClientKeepAliveIntervalSeconds(Integer.parseInt(upstreamClientKeepAliveIntervalSeconds));
            log.info("Override upstreamClientKeepAliveIntervalSeconds from system property: {}",
                    upstreamClientKeepAliveIntervalSeconds);
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
