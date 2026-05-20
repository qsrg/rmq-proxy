package com.mq.proxy.test;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;
import java.util.Set;

public class RocketMQPullConsumerTest {
    
    private static final String PROXY_ADDR = "127.0.0.1:10913";
    private static final String TOPIC = "TestTopic";
    private static final String PRODUCER_GROUP = "TestProducerGroup";
    private static final String CONSUMER_GROUP = "TestConsumerGroup";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== RocketMQ 消息收发测试（PullConsumer）===");
        System.out.println();
        System.out.println("配置信息:");
        System.out.println("  NameServer: 127.0.0.1:9876");
        System.out.println("  Broker: 127.0.0.1:10911");
        System.out.println("  Proxy: " + PROXY_ADDR);
        System.out.println("  Topic: " + TOPIC);
        System.out.println();
        System.out.println("注意：使用 PullConsumer 避免 Rebalance 问题");
        System.out.println();
        
        testProducer();
        
        Thread.sleep(1000);
        
        testPullConsumer();
        
        System.out.println("=== 测试完成 ===");
    }
    
    private static void testProducer() throws Exception {
        System.out.println("【步骤1】创建 Producer，连接 Proxy");
        
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(PROXY_ADDR);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        
        producer.start();
        System.out.println("✓ Producer 启动成功");
        System.out.println();
        
        System.out.println("【步骤2】发送 5 条测试消息");
        for (int i = 0; i < 5; i++) {
            Message msg = new Message(TOPIC, "TagA", 
                ("Hello PullConsumer Test - Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            
            SendResult result = producer.send(msg);
            System.out.println("消息 " + i + ": 发送成功");
            System.out.println("  msgId: " + result.getMsgId());
            System.out.println("  queueId: " + result.getMessageQueue().getQueueId());
            System.out.println("  queueOffset: " + result.getQueueOffset());
        }
        
        producer.shutdown();
        System.out.println();
        System.out.println("✓ Producer 已关闭");
        System.out.println();
    }
    
    private static void testPullConsumer() throws Exception {
        System.out.println("【步骤3】创建 PullConsumer，连接 Proxy");
        System.out.println("  说明：PullConsumer 不需要 Rebalance，直接拉取消息");
        System.out.println();
        
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(PROXY_ADDR);
        
        consumer.start();
        System.out.println("✓ PullConsumer 启动成功");
        System.out.println();
        
        System.out.println("【步骤4】拉取消息");
        
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(TOPIC);
        System.out.println("获取到的队列数量: " + mqs.size());
        System.out.println();
        
        int totalReceived = 0;
        
        for (MessageQueue mq : mqs) {
            System.out.println("拉取队列: queueId=" + mq.getQueueId());
            
            long offset = 0;
            
            for (int i = 0; i < 3; i++) {
                PullResult result = consumer.pull(mq, "TagA", offset, 32);
                
                System.out.println("  拉取结果: " + result.getPullStatus());
                System.out.println("  nextBeginOffset: " + result.getNextBeginOffset());
                System.out.println("  minOffset: " + result.getMinOffset());
                System.out.println("  maxOffset: " + result.getMaxOffset());
                
                if (result.getPullStatus() == PullStatus.FOUND) {
                    List<MessageExt> msgs = result.getMsgFoundList();
                    System.out.println("  收到消息数: " + msgs.size());
                    
                    for (MessageExt msg : msgs) {
                        totalReceived++;
                        System.out.println("    消息 #" + totalReceived + ":");
                        System.out.println("      msgId: " + msg.getMsgId());
                        System.out.println("      body: " + new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                        System.out.println("      queueId: " + msg.getQueueId());
                        System.out.println("      queueOffset: " + msg.getQueueOffset());
                    }
                    
                    offset = result.getNextBeginOffset();
                } else if (result.getPullStatus() == PullStatus.NO_NEW_MSG) {
                    System.out.println("  没有新消息");
                    break;
                } else if (result.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
                    System.out.println("  没有匹配的消息");
                    offset = result.getNextBeginOffset();
                } else if (result.getPullStatus() == PullStatus.OFFSET_ILLEGAL) {
                    System.out.println("  offset 非法");
                    offset = result.getNextBeginOffset();
                }
                
                System.out.println();
            }
        }
        
        System.out.println();
        System.out.println("✓ 总共收到 " + totalReceived + " 条消息");
        
        consumer.shutdown();
        System.out.println("✓ PullConsumer 已关闭");
    }
}