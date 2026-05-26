package com.mq.proxy.example.producer;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;

/**
 * 批量发送消息示例
 */
public class BatchProducer {

    public static void main(String[] args) {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("BatchProducerGroup");
        config.setRetryTimes(3);

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ Producer 启动成功");
            System.out.println();

            String topic = "BatchProducerTopic";
            String tags = "TagBatch";

            System.out.println("批量发送10条消息:");

            long startTime = System.currentTimeMillis();
            int messageCount = 10;
            int successCount = 0;

            for (int i = 0; i < messageCount; i++) {
                String key = "BatchKey_" + i;
                String body = "Batch Message #" + i;

                SendResult result = client.send(topic, tags, key, body.getBytes());

                if (result.isSuccess()) {
                    successCount++;
                    System.out.printf("[%02d] msgId=%s%n", i, result.getMsgId());
                }
            }

            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println();
            System.out.println("统计:");
            System.out.println("  总消息数: " + messageCount);
            System.out.println("  成功数: " + successCount);
            System.out.println("  总耗时: " + elapsed + " ms");
            System.out.println("  TPS: " + String.format("%.2f", messageCount * 1000.0 / elapsed));

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println("✓ Producer 已关闭");
        }
    }
}