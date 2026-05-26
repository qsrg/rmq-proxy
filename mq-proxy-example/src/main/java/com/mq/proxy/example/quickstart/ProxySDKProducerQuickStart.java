package com.mq.proxy.example.quickstart;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;

import java.util.Map;

/**
 * Proxy SDK 生产者快速开始示例
 */
public class ProxySDKProducerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  Proxy SDK 生产者快速开始示例");
        System.out.println("========================================");
        System.out.println();

        // 创建配置
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("QuickStartProducerGroup");
        config.setRetryTimes(3);
        config.setRequestTimeoutMillis(3000);
        config.setEnableMetrics(true);
        config.setEnableTrace(true);

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ ProxyClient 启动成功");
            System.out.println();

            // 发送单条消息
            String topic = "QuickStartTopic";
            String tags = "TagA";
            String keys = "OrderID_123";  // keys是String类型
            String messageBody = "Hello Proxy SDK!";

            SendResult result = client.send(topic, tags, keys, messageBody.getBytes());

            if (result.isSuccess()) {
                System.out.println("✓ 消息发送成功!");
                System.out.println("  消息ID: " + result.getMsgId());
                System.out.println("  队列ID: " + result.getQueueId());
                System.out.println("  队列偏移量: " + result.getQueueOffset());
                System.out.println("  TraceID: " + result.getTraceId());
            } else {
                System.out.println("✗ 消息发送失败: " + result.getErrorMsg());
            }
            System.out.println();

            // 批量发送
            System.out.println("批量发送10条消息...");
            int successCount = 0;
            for (int i = 0; i < 10; i++) {
                String msgBody = "Batch Message #" + i;
                String msgKey = "Key_" + i;  // String类型

                SendResult batchResult = client.send(topic, tags, msgKey, msgBody.getBytes());

                if (batchResult.isSuccess()) {
                    successCount++;
                    System.out.println("[" + i + "] msgId=" + batchResult.getMsgId());
                }
            }
            System.out.println("✓ 批量发送完成: 成功 " + successCount + "/10 条");
            System.out.println();

            // 获取监控数据
            Map<String, ProxyMetricsSnapshot> metrics = client.getMetrics();
            for (Map.Entry<String, ProxyMetricsSnapshot> entry : metrics.entrySet()) {
                ProxyMetricsSnapshot snapshot = entry.getValue();
                System.out.println("代理: " + snapshot.getProxyAddr());
                System.out.println("  成功: " + snapshot.getSuccessCount());
                System.out.println("  失败: " + snapshot.getFailureCount());
                System.out.println("  成功率: " + String.format("%.2f%%", snapshot.getSuccessRate() * 100));
                System.out.println("  平均延迟: " + snapshot.getAvgElapsedMillis() + " ms");
            }

        } catch (ProxyException e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println("✓ ProxyClient 已关闭");
        }
    }
}