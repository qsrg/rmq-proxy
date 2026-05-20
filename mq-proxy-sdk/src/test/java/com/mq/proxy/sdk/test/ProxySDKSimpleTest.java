package com.mq.proxy.sdk.test;

import com.mq.proxy.sdk.facade.ProxyAddressManager;
import com.mq.proxy.sdk.client.ProxyClientConfig;

public class ProxySDKSimpleTest {
    
    public static void main(String[] args) {
        System.out.println("=== proxy-SDK 简化测试 ===");
        System.out.println();
        
        testAddressManager();
    }
    
    private static void testAddressManager() {
        System.out.println("【测试1】地址管理器测试");
        System.out.println();
        
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911;127.0.0.1:11912;127.0.0.1:11913");
        config.setFaultIsolationDurationMillis(30000);
        
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        System.out.println("配置的 Proxy 地址列表:");
        for (String addr : manager.getProxyAddrList()) {
            System.out.println("  - " + addr);
        }
        System.out.println();
        
        System.out.println("测试轮询选择（连续调用5次）:");
        for (int i = 0; i < 5; i++) {
            String addr = manager.selectProxyAddr();
            System.out.println("  选择" + (i + 1) + ": " + addr);
        }
        System.out.println();
        
        System.out.println("测试故障隔离:");
        String addr1 = manager.selectProxyAddr();
        System.out.println("  选择地址: " + addr1);
        manager.markFault(addr1);
        System.out.println("  标记故障: " + addr1);
        
        String addr2 = manager.selectProxyAddr();
        System.out.println("  再次选择: " + addr2);
        if (!addr1.equals(addr2)) {
            System.out.println("  ✓ 故障隔离生效，跳过了故障地址");
        } else {
            System.out.println("  ✗ 故障隔离未生效");
        }
        System.out.println();
        
        System.out.println("测试故障恢复:");
        manager.clearFault(addr1);
        System.out.println("  清除故障: " + addr1);
        addr2 = manager.selectProxyAddr();
        System.out.println("  再次选择: " + addr2);
        if (addr1.equals(addr2)) {
            System.out.println("  ✓ 故障恢复生效，可以再次选择该地址");
        }
        System.out.println();
        
        System.out.println("测试完成!");
    }
}