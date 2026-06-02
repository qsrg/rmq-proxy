package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.config.ProxyCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyFaultDetector {

    private static final Logger log = LoggerFactory.getLogger(ProxyFaultDetector.class);

    private final ProxyCommonConfig config;
    private final ProxyAddressManager addressManager;
    private final ProxyChannelManager channelManager;
    private final ScheduledExecutorService scheduler;

    ProxyFaultDetector(ProxyCommonConfig config, ProxyAddressManager addressManager,
                       ProxyChannelManager channelManager) {
        this.config = config;
        this.addressManager = addressManager;
        this.channelManager = channelManager;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ProxyFaultDetector");
            t.setDaemon(true);
            return t;
        });
    }

    void start() {
        long interval = config.getFaultDetectorIntervalMillis();
        scheduler.scheduleAtFixedRate(this::detect, interval, interval, TimeUnit.MILLISECONDS);
    }

    void shutdown() {
        scheduler.shutdown();
    }

    void detect() {
        try {
            List<String> addrList = addressManager.getProxyAddrList();
            Map<String, ProxyAddressManager.FaultItem> faultTable = addressManager.getFaultItemTable();

            for (String addr : addrList) {
                ProxyAddressManager.FaultItem item = faultTable.get(addr);
                if (item == null) {
                    continue;
                }

                if (!item.isAvailable()) {
                    boolean reachable = probe(addr);
                    if (reachable) {
                        addressManager.markReachable(addr);
                        log.info("Proxy fault detector: {} is reachable now, will be used after isolation expires", addr);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Proxy fault detector error", e);
        }
    }

    private boolean probe(String addr) {
        try {
            ProxyChannelManager.ProbeResult result = channelManager.probe(
                    addr,
                    config.getConnectTimeoutMillis(),
                    config.isTlsEnabled()
            );
            return result.isActive() && (!config.isTlsEnabled() || result.isTlsReady());
        } catch (Exception e) {
            return false;
        }
    }
}
