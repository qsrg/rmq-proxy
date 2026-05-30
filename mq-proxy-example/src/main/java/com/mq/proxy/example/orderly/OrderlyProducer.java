package com.mq.proxy.example.orderly;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class OrderlyProducer {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  顺序消息生产者示例 (原生RocketMQ客户端)");
        System.out.println("========================================");
        System.out.println();

        DefaultMQProducer producer = new DefaultMQProducer("OrderlyProducerGroup");
        producer.setNamesrvAddr("127.0.0.1:19876");
        producer.setRetryTimesWhenSendFailed(3);

        try {
            producer.start();
            System.out.println("Producer 启动成功");
            System.out.println();

            String topic = "OrderlyTopic";
            String[] orderIds = {"Order_001", "Order_002", "Order_003"};

            for (int round = 0; round < 3; round++) {
                for (String orderId : orderIds) {
                    String msgBody = orderId + " -> Step " + round;
                    Message msg = new Message(topic, "TagA", orderId, msgBody.getBytes());

                    SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            String id = (String) arg;
                            int index = Math.abs(id.hashCode()) % mqs.size();
                            return mqs.get(index);
                        }
                    }, orderId);

                    System.out.println("发送成功: " + msgBody
                            + ", queueId=" + sendResult.getMessageQueue().getQueueId()
                            + ", msgId=" + sendResult.getMsgId());
                }
            }

            System.out.println();
            System.out.println("========================================");
            System.out.println("顺序消息发送完成!");
            System.out.println("同一订单ID的消息发送到同一队列，保证消费顺序");
            System.out.println("========================================");

        } catch (MQClientException e) {
            System.out.println("客户端异常: " + e.getMessage());
            System.out.println("提示：请确保Proxy服务已启动（127.0.0.1:19876）");
        } catch (Exception e) {
            System.out.println("发送异常: " + e.getMessage());
        } finally {
            producer.shutdown();
            System.out.println("Producer 已关闭");
        }
    }
}
