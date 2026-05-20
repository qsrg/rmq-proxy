package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyAddressManager {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyAddressManager.class);
    
    private final ProxyClientConfig config;
    
    private final List<String> proxyAddrList;
    
    private final AtomicInteger index = new AtomicInteger(0);
    
    private final ConcurrentHashMap<String, Long> faultAddrTable = new ConcurrentHashMap<>();
    
    public ProxyAddressManager(ProxyClientConfig config) {
        this.config = config;
        this.proxyAddrList = parseProxyAddrs(config.getProxyAddrs());
        log.info("Proxy address list initialized: {}", proxyAddrList);
    }
    
    public String selectProxyAddr() {
        if (proxyAddrList.isEmpty()) {
            return null;
        }
        
        for (int i = 0; i < proxyAddrList.size(); i++) {
            int currentIndex = Math.abs(index.incrementAndGet()) % proxyAddrList.size();
            String addr = proxyAddrList.get(currentIndex);
            
            if (!isInFaultIsolation(addr)) {
                return addr;
            }
        }
        
        return null;
    }
    
    public void markFault(String addr) {
        faultAddrTable.put(addr, System.currentTimeMillis());
        log.warn("Proxy address marked as fault: {}", addr);
    }
    
    public void clearFault(String addr) {
        Long faultTime = faultAddrTable.remove(addr);
        if (faultTime != null) {
            log.info("Proxy address recovered from fault: {}", addr);
        }
    }
    
    private boolean isInFaultIsolation(String addr) {
        Long faultTime = faultAddrTable.get(addr);
        if (faultTime == null) {
            return false;
        }
        
        long elapsed = System.currentTimeMillis() - faultTime;
        if (elapsed >= config.getFaultIsolationDurationMillis()) {
            faultAddrTable.remove(addr);
            log.info("Proxy address fault isolation expired: {}", addr);
            return false;
        }
        
        return true;
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
}
