package com.mq.proxy.example.consumer;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.PullResult;
import com.mq.proxy.sdk.exception.ProxyException;

/**
 * Pull 消费者示例
 */
public class PullConsumer {

    public static void main(String[] args) {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("PullConsumerGroup");  // consumerGroup也用这个配置
        config.setSuspendTimeoutMillis(15000);  // 长轮询15秒

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ Consumer 启动成功");
            System.out.println();

            String topic = "PullConsumerTopic";
            String consumerGroup = "PullConsumerGroup";
            int queueId = 0;  // queueId是int类型
            long offset = 0;  // 从偏移量0开始
            int maxNums = 32;  // 每次拉取32条

            System.out.println("Pull参数:");
            System.out.println("  Topic: " + topic);
            System.out.println("  ConsumerGroup: " + consumerGroup);
            System.out.println("  QueueId: " + queueId);
            System.out.println("  Offset: " + offset);
            System.out.println("  MaxNums: " + maxNums);
            System.out.println();

            System.out.println("长轮询拉取10轮:");

            for (int round = 0; round < 10; round++) {
                System.out.println("--- 拉取 #" + round + " ---");

                try {
                    // API: pull(topic, consumerGroup, queueId, offset, maxNums)
                    PullResult pullResult = client.pull(topic, consumerGroup, queueId, offset, maxNums);

                    if (pullResult != null && pullResult.isFound()) {
                        byte[] body = pullResult.getBody();

                        if (body != null && body.length > 0) {
                            String messageContent = new String(body);
                            System.out.println("拉取成功:");
                            System.out.println("  内容: " + messageContent);
                            System.out.println("  NextOffset: " + pullResult.getNextBeginOffset());

                            // 更新偏移量
                            offset = pullResult.getNextBeginOffset();
                        }
                    } else {
                        System.out.println("没有新消息");
                    }

                } catch (ProxyException e) {
                    System.out.println("拉取异常: " + e.getMessage());
                }

                System.out.println();
                Thread.sleep(500);
            }

            System.out.println("✓ Pull消费示例完成");

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println("✓ Consumer 已关闭");
        }
    }
}