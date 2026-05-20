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

public class ConsumerTest {
    
    private static final String PROXY_ADDR = "127.0.0.1:10913";
    private static final String TOPIC = "ConsumerTestTopic";
    private static final String PRODUCER_GROUP = "ConsumerTestProducerGroup";
    private static final String CONSUMER_GROUP = "ConsumerTestConsumerGroup";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== 消费者功能测试 ===");
        System.out.println();
        System.out.println("配置信息:");
        System.out.println("  Proxy地址: " + PROXY_ADDR);
        System.out.println("  Topic: " + TOPIC);
        System.out.println("  ProducerGroup: " + PRODUCER_GROUP);
        System.out.println("  ConsumerGroup: " + CONSUMER_GROUP);
        System.out.println();
        
        testProducer();
        
        Thread.sleep(1000);
        
        testPullConsumer();
        
        System.out.println();
        System.out.println("=== 测试完成 ===");
    }
    
    private static void testProducer() throws Exception {
        System.out.println("【步骤1】发送消息");
        
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP);
        producer.setNamesrvAddr(PROXY_ADDR);
        producer.setRetryTimesWhenSendFailed(3);
        producer.setSendMsgTimeout(5000);
        
        producer.start();
        System.out.println("Producer启动成功");
        
        int successCount = 0;
        for (int i = 0; i < 5; i++) {
            Message msg = new Message(TOPIC, "TagA", 
                ("Consumer Test Message " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            
            try {
                SendResult result = producer.send(msg);
                System.out.println("消息" + i + ": SEND_OK, msgId=" + result.getMsgId() + 
                    ", queueId=" + result.getMessageQueue().getQueueId() +
                    ", offset=" + result.getQueueOffset());
                successCount++;
            } catch (Exception e) {
                System.out.println("消息" + i + ": 发送失败 - " + e.getMessage());
            }
        }
        
        producer.shutdown();
        System.out.println("Producer已关闭");
        System.out.println("发送成功: " + successCount + "/5");
        System.out.println();
    }
    
    private static void testPullConsumer() throws Exception {
        System.out.println("【步骤2】拉取消息");
        
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(CONSUMER_GROUP);
        consumer.setNamesrvAddr(PROXY_ADDR);
        
        consumer.start();
        System.out.println("PullConsumer启动成功");
        
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(TOPIC);
        System.out.println("获取队列数量: " + mqs.size());
        
        int totalReceived = 0;
        int maxAttempts = 3;
        
        for (MessageQueue mq : mqs) {
            System.out.println("拉取队列: queueId=" + mq.getQueueId());
            
            long offset = 0;
            
            for (int attempt = 0; attempt < maxAttempts; attempt++) {
                try {
                    PullResult result = consumer.pull(mq, "*", offset, 32);
                    
                    System.out.println("  拉取结果: " + result.getPullStatus());
                    System.out.println("  nextBeginOffset: " + result.getNextBeginOffset());
                    System.out.println("  minOffset: " + result.getMinOffset());
                    System.out.println("  maxOffset: " + result.getMaxOffset());
                    
                    if (result.getPullStatus() == PullStatus.FOUND) {
                        List<MessageExt> msgs = result.getMsgFoundList();
                        System.out.println("  收到消息数: " + msgs.size());
                        
                        for (MessageExt msg : msgs) {
                            totalReceived++;
                            System.out.println("    消息#" + totalReceived + ": " + 
                                new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET));
                        }
                        
                        offset = result.getNextBeginOffset();
                    } else if (result.getPullStatus() == PullStatus.NO_NEW_MSG) {
                        System.out.println("  没有新消息");
                        break;
                    } else if (result.getPullStatus() == PullStatus.OFFSET_ILLEGAL) {
                        System.out.println("  offset非法，重置到: " + result.getNextBeginOffset());
                        offset = result.getNextBeginOffset();
                    } else {
                        System.out.println("  其他状态: " + result.getPullStatus());
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("  拉取异常: " + e.getMessage());
                    if (e.getMessage().contains("CODE:")) {
                        System.out.println("  错误码解析: " + parseErrorCode(e.getMessage()));
                    }
                    break;
                }
                
                Thread.sleep(100);
            }
        }
        
        consumer.shutdown();
        System.out.println("PullConsumer已关闭");
        System.out.println("总共收到消息: " + totalReceived);
    }
    
    private static String parseErrorCode(String message) {
        if (message.contains("CODE: 24")) {
            return "SUBSCRIPTION_NOT_EXIST - 订阅信息不存在";
        } else if (message.contains("CODE: 17")) {
            return "TOPIC_NOT_EXIST - Topic不存在";
        } else if (message.contains("CODE: 19")) {
            return "PULL_NOT_FOUND - 没有找到消息";
        } else if (message.contains("CODE: 206")) {
            return "CONSUMER_NOT_ONLINE - 消费者不在线";
        }
        return message;
    }
}