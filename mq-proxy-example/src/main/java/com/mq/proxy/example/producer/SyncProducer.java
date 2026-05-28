package com.mq.proxy.example.producer;

import com.mq.proxy.sdk.producer.ProxyProducer;
import com.mq.proxy.sdk.producer.SendResult;

/**
 * 同步发送消息示例
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        ProxyProducer producer = new ProxyProducer("SyncProducerGroup")
            .setProxyAddrs("127.0.0.1:11911");

        producer.start();

        for (int i = 0; i < 10; i++) {
            SendResult result = producer.send("TestTopic", "TagA", 
                "Key" + i, ("Message " + i).getBytes());
            
            System.out.println("Send result: " + result.isSuccess() + 
                ", msgId=" + result.getMsgId());
        }

        producer.shutdown();
    }
}
