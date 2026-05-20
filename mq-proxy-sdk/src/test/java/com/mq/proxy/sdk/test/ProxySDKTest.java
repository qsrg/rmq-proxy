package com.mq.proxy.sdk.test;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;

import java.util.Map;

public class ProxySDKTest {
    
    public static void main(String[] args) {
        System.out.println("=== proxy-SDK 测试程序 ===");
        System.out.println();
        
        testBasicConnection();
    }
    
    private static void testBasicConnection() {
        System.out.println("【测试1】基本连接测试");
        System.out.println("目标: 测试 proxy-SDK 连接到 mq-proxy 服务");
        System.out.println();
        
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setConnectTimeoutMillis(5000);
        config.setRequestTimeoutMillis(5000);
        config.setRetryTimes(2);
        config.setEnableMetrics(true);
        config.setEnableTrace(true);
        
        ProxyClient client = new ProxyClient(config);
        
        try {
            System.out.println("启动 ProxyClient...");
            client.start();
            System.out.println("✓ ProxyClient 启动成功");
            System.out.println();
            
            System.out.println("发送测试消息...");
            String topic = "TestTopic";
            String tags = "TestTag";
            String message = "Hello from proxy-SDK!";
            
            SendResult result = client.send(topic, tags, message.getBytes());
            
            if (result.isSuccess()) {
                System.out.println("✓ 消息发送成功!");
                System.out.println("  msgId: " + result.getMsgId());
                System.out.println("  traceId: " + result.getTraceId());
                System.out.println();
            } else {
                System.out.println("✗ 消息发送失败: " + result.getErrorMsg());
            }
            
            System.out.println("获取监控数据...");
            Map<String, ProxyMetricsSnapshot> metrics = client.getMetrics();
            
            for (Map.Entry<String, ProxyMetricsSnapshot> entry : metrics.entrySet()) {
                ProxyMetricsSnapshot snap = entry.getValue();
                System.out.println("Proxy: " + snap.getProxyAddr());
                System.out.println("  成功次数: " + snap.getSuccessCount());
                System.out.println("  失败次数: " + snap.getFailureCount());
                System.out.println("  成功率: " + String.format("%.2f%%", snap.getSuccessRate() * 100));
                System.out.println("  平均延迟: " + snap.getAvgElapsedMillis() + "ms");
            }
            
        } catch (ProxyException e) {
            System.out.println("✗ 测试失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println();
            System.out.println("关闭 ProxyClient...");
            client.shutdown();
            System.out.println("✓ ProxyClient 已关闭");
        }
    }
}