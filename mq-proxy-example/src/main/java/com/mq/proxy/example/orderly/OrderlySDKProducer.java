package com.mq.proxy.example.orderly;

import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.producer.ProxyProducer;
import com.mq.proxy.sdk.producer.SendResult;

public class OrderlySDKProducer {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  顺序消息生产者示例 (Proxy SDK)");
        System.out.println("========================================");
        System.out.println();

        ProxyProducer producer = new ProxyProducer("OrderlySDKProducerGroup")
            .setProxyAddrs("127.0.0.1:19876")
            .setRetryTimes(3);

        try {
            producer.start();
            System.out.println("ProxyProducer 启动成功");
            System.out.println();

            String topic = "OrderlyTopic";

            String[] orderIds = {"Order_001", "Order_002", "Order_003"};
            int queueCount = 4;

            for (int round = 0; round < 3; round++) {
                for (String orderId : orderIds) {
                    String msgBody = orderId + " -> Step " + round;
                    String tags = "TagA";
                    String keys = orderId + "_step" + round;

                    int queueId = Math.abs(orderId.hashCode()) % queueCount;

                    SendResult result = producer.send(topic, tags, keys, msgBody.getBytes(), 0, queueId);

                    if (result.isSuccess()) {
                        System.out.println("发送成功: " + msgBody
                                + ", queueId=" + result.getQueueId()
                                + ", msgId=" + result.getMsgId());
                    } else {
                        System.out.println("发送失败: " + msgBody + ", error=" + result.getErrorMsg());
                    }
                }
            }

            System.out.println();
            System.out.println("========================================");
            System.out.println("顺序消息发送完成!");
            System.out.println("通过指定queueId，同一订单的消息发送到同一队列");
            System.out.println("========================================");

        } catch (ProxyException e) {
            System.out.println("发送异常: " + e.getMessage());
        } finally {
            producer.shutdown();
            System.out.println("ProxyProducer 已关闭");
        }
    }
}
