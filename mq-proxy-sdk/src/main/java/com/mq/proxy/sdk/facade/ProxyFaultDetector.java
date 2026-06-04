package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.config.ProxyCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyFaultDetector {

    private static final Logger log = LoggerFactory.getLogger(ProxyFaultDetector.class);

    private final ProxyCommonConfig config;
    private final ProxyAddressManager addressManager;
    private final ProxyChannelManager channelManager;
    private final ScheduledExecutorService scheduler;

    public ProxyFaultDetector(ProxyCommonConfig config,
                              ProxyAddressManager addressManager,
                              ProxyChannelManager channelManager) {
        this.config = config;
        this.addressManager = addressManager;
        this.channelManager = channelManager;
        this.scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "FaultDetectorThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void start() {
        long interval = config.getFaultDetectorIntervalMillis();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                detect();
            } catch (Exception e) {
                log.error("Fault detection error", e);
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
        log.info("Fault detector started, interval={}ms", interval);
    }

    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void detect() {
        for (String addr : addressManager.getProxyAddrList()) {
            if (!addressManager.isAvailable(addr)) {
                ProxyChannelManager.ProbeResult result = channelManager.probe(
                        addr, config.getConnectTimeoutMillis(), config.isTlsEnabled());
                if (result.isActive()) {
                    addressManager.markReachable(addr);
                    log.info("Fault detector marked proxy as reachable: {}", addr);
                }
            }
        }
    }
}
