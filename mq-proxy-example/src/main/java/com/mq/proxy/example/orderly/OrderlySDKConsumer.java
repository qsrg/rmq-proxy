package com.mq.proxy.example.orderly;

import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.PullResult;
import com.mq.proxy.sdk.exception.ProxyException;

public class OrderlySDKConsumer {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  顺序消息消费者示例 (Proxy SDK)");
        System.out.println("========================================");
        System.out.println();

        ProxyConsumer consumer = new ProxyConsumer("OrderlySDKConsumerGroup")
            .setProxyAddrs("127.0.0.1:19876")
            .setRetryTimes(3);

        try {
            consumer.start();
            System.out.println("ProxyConsumer 启动成功");
            System.out.println();

            String topic = "OrderlyTopic";
            String consumerGroup = "OrderlySDKConsumerGroup";
            int queueCount = 4;
            long[] offsets = new long[queueCount];
            for (int i = 0; i < queueCount; i++) {
                offsets[i] = -1;
            }

            System.out.println("开始顺序消费...");
            System.out.println();

            int totalConsumed = 0;
            int maxRounds = 20;

            for (int round = 0; round < maxRounds; round++) {
                boolean hasNewMessage = false;

                for (int queueId = 0; queueId < queueCount; queueId++) {
                    long offset = offsets[queueId];
                    if (offset < 0) {
                        offset = consumer.queryConsumerOffset(consumerGroup, topic, queueId);
                        if (offset < 0) {
                            offset = 0;
                        }
                        offsets[queueId] = offset;
                    }

                    try {
                        long commitOffset = offsets[queueId];
                        PullResult pullResult = consumer.pull(topic, consumerGroup, queueId, offset, 32, commitOffset);

                        if (pullResult != null && pullResult.isFound()) {
                            hasNewMessage = true;
                            byte[] body = pullResult.getBody();
                            if (body != null && body.length > 0) {
                                String content = new String(body);
                                System.out.println("  队列" + queueId + ": " + content
                                        + ", offset=" + offset + "->" + pullResult.getNextBeginOffset());
                                offsets[queueId] = pullResult.getNextBeginOffset();
                                totalConsumed++;
                            }
                        }
                    } catch (ProxyException e) {
                        System.out.println("  队列" + queueId + " 拉取异常: " + e.getMessage());
                    }
                }

                if (!hasNewMessage) {
                    System.out.println("  轮次" + round + ": 无新消息");
                }

                Thread.sleep(1000);
            }

            for (int queueId = 0; queueId < queueCount; queueId++) {
                if (offsets[queueId] > 0) {
                    try {
                        consumer.updateConsumerOffset(consumerGroup, topic, queueId, offsets[queueId]);
                    } catch (ProxyException e) {
                        System.out.println("更新offset异常: queueId=" + queueId + ", " + e.getMessage());
                    }
                }
            }

            System.out.println();
            System.out.println("========================================");
            System.out.println("消费完成，总计: " + totalConsumed + " 条");
            System.out.println("顺序消费通过指定queueId拉取，并在Pull时提交commitOffset");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("消费异常: " + e.getMessage());
        } finally {
            consumer.shutdown();
            System.out.println("ProxyConsumer 已关闭");
        }
    }
}
