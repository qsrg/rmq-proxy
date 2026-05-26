package com.mq.proxy.example.producer;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;

/**
 * 同步发送消息示例
 */
public class SyncProducer {

    public static void main(String[] args) {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911");
        config.setProducerGroup("SyncProducerGroup");
        config.setRetryTimes(3);

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ Producer 启动成功");
            System.out.println();

            String topic = "SyncProducerTopic";
            String tags = "TagA";

            // 发送单条消息
            String keys = "SyncKey_001";
            String body = "This is a synchronous message";

            SendResult result = client.send(topic, tags, keys, body.getBytes());

            System.out.println("发送结果:");
            System.out.println("  成功: " + result.isSuccess());
            System.out.println("  消息ID: " + result.getMsgId());
            System.out.println("  队列ID: " + result.getQueueId());
            System.out.println();

            // 发送多条消息
            System.out.println("连续发送10条消息:");
            for (int i = 0; i < 10; i++) {
                String msgBody = "Sync Message #" + i;
                String msgKey = "SyncKey_" + i;

                SendResult sendResult = client.send(topic, tags, msgKey, msgBody.getBytes());

                if (sendResult.isSuccess()) {
                    System.out.printf("[%02d] msgId=%s, queue=%d%n",
                        i, sendResult.getMsgId(), sendResult.getQueueId());
                }

                Thread.sleep(100);
            }

            System.out.println("✓ 同步发送示例完成");

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println("✓ Producer 已关闭");
        }
    }
}