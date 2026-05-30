package com.mq.proxy.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 原生 RocketMQ 生产者快速开始示例
 *
 * 通过Proxy发送消息到RocketMQ
 */
public class RocketMQProducerQuickStart {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  原生 RocketMQ 生产者快速开始示例");
        System.out.println("========================================");
        System.out.println();

        // Step 1: 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("QuickStartProducerGroup");

        // Step 2: 设置 NameServer 地址为 Proxy 地址
        producer.setNamesrvAddr("127.0.0.1:19876");  // 通过Proxy访问RocketMQ

        // 可选设置
        producer.setRetryTimesWhenSendFailed(3);  // 发送失败重试次数
        producer.setSendMsgTimeout(3000);  // 发送超时时间

        try {
            // Step 3: 启动生产者
            System.out.println("【步骤1】启动 DefaultMQProducer...");
            producer.start();
            System.out.println("✓ Producer 启动成功");
            System.out.println();

            // Step 4: 发送消息
            System.out.println("【步骤2】发送测试消息...");
            String topic = "QuickStartTopic";
            String tags = "TagA";
            String keys = "OrderID_123";
            String messageBody = "Hello RocketMQ! This is a native client example.";

            Message msg = new Message(
                topic,
                tags,
                keys,
                messageBody.getBytes()
            );

            SendResult sendResult = producer.send(msg);

            System.out.println("✓ 消息发送成功!");
            System.out.println("  - 消息ID: " + sendResult.getMsgId());
            System.out.println("  - 发送状态: " + sendResult.getSendStatus());
            System.out.println("  - 消息队列: " + sendResult.getMessageQueue());
            System.out.println("  - 队列偏移量: " + sendResult.getQueueOffset());
            System.out.println();

            // Step 5: 批量发送
            System.out.println("【步骤3】批量发送10条消息...");
            int successCount = 0;
            for (int i = 0; i < 10; i++) {
                String msgBody = "Batch Message #" + i;
                Message batchMsg = new Message(
                    topic,
                    tags,
                    "Key_" + i,
                    msgBody.getBytes()
                );

                SendResult batchResult = producer.send(batchMsg);
                successCount++;
                System.out.println("  [" + i + "] 发送成功: msgId=" + batchResult.getMsgId());
            }
            System.out.println("✓ 批量发送完成: 成功 " + successCount + "/10 条");
            System.out.println();

            System.out.println("========================================");
            System.out.println("✓ 原生客户端示例执行成功!");
            System.out.println("========================================");

        } catch (MQClientException e) {
            System.out.println("✗ 客户端异常: " + e.getMessage());
            System.out.println("提示：请确保Proxy服务已启动（127.0.0.1:19876）");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("✗ 发送异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Step 6: 关闭生产者
            System.out.println();
            System.out.println("【步骤4】关闭 Producer...");
            producer.shutdown();
            System.out.println("✓ Producer 已关闭");
        }
    }
}