package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.config.ProxyCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyAddressManager {

    private static final Logger log = LoggerFactory.getLogger(ProxyAddressManager.class);

    private final ProxyCommonConfig config;
    private final List<String> proxyAddrList;
    private final AtomicInteger index = new AtomicInteger(0);
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<>();

    private volatile ProxyFaultDetector faultDetector;

    public ProxyAddressManager(ProxyCommonConfig config) {
        this.config = config;
        validateLatencyConfig(config);
        this.proxyAddrList = parseProxyAddrs(config.getProxyAddrs());
        log.info("Proxy address list initialized: {}", proxyAddrList);
    }

    public void startDetector(ProxyChannelManager channelManager) {
        if (!config.isEnableFaultDetector() || proxyAddrList.size() <= 1) {
            return;
        }
        this.faultDetector = new ProxyFaultDetector(config, this, channelManager);
        this.faultDetector.start();
        log.info("Proxy fault detector started, interval={}ms", config.getFaultDetectorIntervalMillis());
    }

    public void shutdownDetector() {
        if (this.faultDetector != null) {
            this.faultDetector.shutdown();
            this.faultDetector = null;
        }
    }

    public String selectProxyAddr() {
        if (proxyAddrList.isEmpty()) {
            return null;
        }

        for (int i = 0; i < proxyAddrList.size(); i++) {
            int currentIndex = indexToMod(index.incrementAndGet(), proxyAddrList.size());
            String addr = proxyAddrList.get(currentIndex);

            if (isAvailable(addr)) {
                return addr;
            }
        }

        for (int i = 0; i < proxyAddrList.size(); i++) {
            int currentIndex = indexToMod(index.incrementAndGet(), proxyAddrList.size());
            String addr = proxyAddrList.get(currentIndex);

            if (isReachableButIsolated(addr)) {
                log.info("Selected reachable-but-isolated proxy: {}", addr);
                return addr;
            }
        }

        return null;
    }

    public void markFault(String addr, long latencyMillis) {
        long notAvailableDuration = computeNotAvailableDuration(latencyMillis);
        if (notAvailableDuration <= 0
                && latencyMillis >= config.getLatencyMax()[config.getLatencyMax().length - 1]) {
            notAvailableDuration = Math.min(config.getFaultIsolationDurationMillis(), 5000L);
        }
        FaultItem item = faultItemTable.get(addr);
        if (item == null) {
            item = new FaultItem(addr);
            FaultItem existing = faultItemTable.putIfAbsent(addr, item);
            if (existing != null) {
                item = existing;
            }
        }
        item.update(latencyMillis, notAvailableDuration);
        log.warn("Proxy address marked as fault: {}, latency={}ms, isolated for {}ms",
                addr, latencyMillis, notAvailableDuration);
    }

    public void markFault(String addr) {
        markFault(addr, config.getFaultIsolationDurationMillis());
    }

    public void clearFault(String addr) {
        FaultItem item = faultItemTable.remove(addr);
        if (item != null) {
            log.info("Proxy address recovered from fault: {}", addr);
        }
    }

    public boolean isAvailable(String addr) {
        FaultItem item = faultItemTable.get(addr);
        if (item == null) {
            return true;
        }
        if (item.isAvailable()) {
            faultItemTable.remove(addr, item);
            log.info("Proxy address fault isolation expired: {}", addr);
            return true;
        }
        return false;
    }

    public boolean isReachable(String addr) {
        FaultItem item = faultItemTable.get(addr);
        if (item == null) {
            return true;
        }
        return item.isReachable();
    }

    public void markReachable(String addr) {
        FaultItem item = faultItemTable.get(addr);
        if (item != null && !item.isReachable()) {
            item.setReachable(true);
            log.info("Proxy address detected reachable: {}", addr);
        }
    }

    private int indexToMod(int value, int size) {
        int mod = value % size;
        return mod >= 0 ? mod : mod + size;
    }

    private boolean isReachableButIsolated(String addr) {
        FaultItem item = faultItemTable.get(addr);
        if (item == null) {
            return false;
        }
        return item.isReachable() && !item.isAvailable();
    }

    private long computeNotAvailableDuration(long currentLatency) {
        long[] latencyMax = config.getLatencyMax();
        long[] notAvailableDuration = config.getNotAvailableDuration();
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i]) {
                return notAvailableDuration[i];
            }
        }
        return 0L;
    }

    private void validateLatencyConfig(ProxyCommonConfig cfg) {
        long[] latencyMax = cfg.getLatencyMax();
        long[] notAvailableDuration = cfg.getNotAvailableDuration();
        if (latencyMax == null || notAvailableDuration == null) {
            throw new IllegalArgumentException("latencyMax and notAvailableDuration must not be null");
        }
        if (latencyMax.length != notAvailableDuration.length) {
            throw new IllegalArgumentException(
                    "latencyMax.length(" + latencyMax.length + ") != notAvailableDuration.length("
                            + notAvailableDuration.length + ")");
        }
        if (latencyMax.length == 0) {
            throw new IllegalArgumentException("latencyMax and notAvailableDuration must not be empty");
        }
        for (int i = 1; i < latencyMax.length; i++) {
            if (latencyMax[i] < latencyMax[i - 1]) {
                throw new IllegalArgumentException(
                        "latencyMax must be non-decreasing, but latencyMax[" + i + "]=" + latencyMax[i]
                                + " < latencyMax[" + (i - 1) + "]=" + latencyMax[i - 1]);
            }
        }
    }

    private List<String> parseProxyAddrs(String proxyAddrs) {
        if (proxyAddrs == null || proxyAddrs.trim().isEmpty()) {
            return Collections.emptyList();
        }

        String[] addrs = proxyAddrs.split(";");
        List<String> list = new ArrayList<>();
        for (String addr : addrs) {
            addr = addr.trim();
            if (!addr.isEmpty()) {
                list.add(addr);
            }
        }

        return Collections.unmodifiableList(list);
    }

    public List<String> getProxyAddrList() {
        return proxyAddrList;
    }

    Map<String, FaultItem> getFaultItemTable() {
        return faultItemTable;
    }

    static class FaultItem {
        private final String name;
        private volatile long currentLatency;
        private volatile long startTimestamp;
        private volatile boolean reachableFlag = true;

        FaultItem(String name) {
            this.name = name;
        }

        void update(long currentLatency, long notAvailableDuration) {
            this.currentLatency = currentLatency;
            if (notAvailableDuration > 0
                    && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
                this.startTimestamp = System.currentTimeMillis() + notAvailableDuration;
            }
            if (notAvailableDuration > 0) {
                this.reachableFlag = false;
            }
        }

        boolean isAvailable() {
            return System.currentTimeMillis() >= startTimestamp;
        }

        boolean isReachable() {
            return reachableFlag;
        }

        void setReachable(boolean reachableFlag) {
            this.reachableFlag = reachableFlag;
        }

        String getName() {
            return name;
        }
    }
}
