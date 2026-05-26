package com.mq.proxy.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;

/**
 * 原生 RocketMQ 消费者快速开始示例
 *
 * 展示使用原生RocketMQ客户端通过Proxy拉取消息
 */
public class RocketMQConsumerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  原生 RocketMQ 消费者快速开始示例");
        System.out.println("========================================");
        System.out.println();

        // 创建消费者
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("ConsumerGroup");

        // 设置NameServer地址为Proxy地址
        consumer.setNamesrvAddr("127.0.0.1:11911");  // Proxy地址

        try {
            consumer.start();
            System.out.println("✓ Consumer 启动成功");
            System.out.println();

            String topic = "QuickStartTopic";

            System.out.println("消费配置:");
            System.out.println("  Topic: " + topic);
            System.out.println("  ConsumerGroup: " + consumer.getConsumerGroup());
            System.out.println("  NamesrvAddr: " + consumer.getNamesrvAddr() + " (Proxy地址)");
            System.out.println();

            // 获取Topic的队列信息
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            System.out.println("Topic " + topic + " 有 " + mqs.size() + " 个队列:");
            for (MessageQueue mq : mqs) {
                System.out.println("  QueueId: " + mq.getQueueId() + ", Broker: " + mq.getBrokerName());
            }
            System.out.println();

            System.out.println("开始消费消息...");
            System.out.println();

            int totalConsumed = 0;
            int maxRounds = 10;

            for (int round = 0; round < maxRounds; round++) {
                System.out.println("--- 消费轮次 #" + round + " ---");

                // 遍历所有队列
                for (MessageQueue mq : mqs) {
                    // 获取当前队列的偏移量
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    System.out.println("队列 " + mq.getQueueId() + " 当前偏移量: " + offset);

                    // 拉取消息
                    PullResult pullResult = consumer.pull(
                        mq,
                        "*",  // 订阅表达式，*表示所有消息
                        offset,
                        32    // 每次拉取32条
                    );

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            System.out.println("队列 " + mq.getQueueId() + " 拉取成功:");
                            System.out.println("  消息数量: " + pullResult.getMsgFoundList().size());

                            for (MessageExt msg : pullResult.getMsgFoundList()) {
                                String body = new String(msg.getBody());
                                System.out.println("    - MsgId: " + msg.getMsgId());
                                System.out.println("      内容: " + body);
                                System.out.println("      QueueOffset: " + msg.getQueueOffset());

                                totalConsumed++;

                                // 模拟处理
                                Thread.sleep(50);
                            }

                            // 更新偏移量
                            consumer.updateConsumeOffset(
                                mq,
                                pullResult.getNextBeginOffset()
                            );
                            System.out.println("  NextOffset: " + pullResult.getNextBeginOffset());
                            break;

                        case NO_NEW_MSG:
                            System.out.println("队列 " + mq.getQueueId() + ": 没有新消息");
                            break;

                        case NO_MATCHED_MSG:
                            System.out.println("队列 " + mq.getQueueId() + ": 没有匹配的消息");
                            break;

                        case OFFSET_ILLEGAL:
                            System.out.println("队列 " + mq.getQueueId() + ": 偏移量非法");
                            break;
                    }
                }

                System.out.println();
                Thread.sleep(500);
            }

            System.out.println("========================================");
            System.out.println("消费统计:");
            System.out.println("  总消费轮数: " + maxRounds);
            System.out.println("  成功消费: " + totalConsumed + " 条");
            System.out.println();
            System.out.println("✓ 消费示例完成");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.shutdown();
            System.out.println();
            System.out.println("✓ Consumer 已关闭");
        }
    }
}