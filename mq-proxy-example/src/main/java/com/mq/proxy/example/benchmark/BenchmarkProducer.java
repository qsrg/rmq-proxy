package com.mq.proxy.example.benchmark;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 性能测试示例
 */
public class BenchmarkProducer {

    public static void main(String[] args) throws InterruptedException {
        final int threadCount = 10;
        final int messageCountPerThread = 1000;
        final int totalMessages = threadCount * messageCountPerThread;

        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911;127.0.0.1:11912");
        config.setProducerGroup("BenchmarkProducerGroup");
        config.setRetryTimes(3);
        config.setEnableMetrics(true);

        ProxyClient client = new ProxyClient(config);
        client.start();

        System.out.println("✓ Producer 启动成功");
        System.out.println();
        System.out.println("测试参数:");
        System.out.println("  并发线程: " + threadCount);
        System.out.println("  每线程消息: " + messageCountPerThread);
        System.out.println("  总消息数: " + totalMessages);
        System.out.println();

        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failureCount = new AtomicLong(0);
        AtomicLong totalElapsedMillis = new AtomicLong(0);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        String topic = "BenchmarkTopic";
        String tags = "TagBenchmark";

        long testStartTime = System.currentTimeMillis();

        for (int threadId = 0; threadId < threadCount; threadId++) {
            new Thread(() -> {
                try {
                    startLatch.await();

                    for (int i = 0; i < messageCountPerThread; i++) {
                        long sendStart = System.currentTimeMillis();

                        try {
                            String key = "Key_" + i;
                            String body = "Benchmark Message #" + i;

                            SendResult result = client.send(topic, tags, key, body.getBytes());

                            long elapsed = System.currentTimeMillis() - sendStart;

                            if (result.isSuccess()) {
                                successCount.incrementAndGet();
                            } else {
                                failureCount.incrementAndGet();
                            }

                            totalElapsedMillis.addAndGet(elapsed);

                        } catch (Exception e) {
                            failureCount.incrementAndGet();
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        endLatch.await();

        long testEndTime = System.currentTimeMillis();
        long testTotalTime = testEndTime - testStartTime;

        System.out.println("性能测试结果:");
        System.out.println();
        System.out.println("统计:");
        System.out.println("  总消息数: " + totalMessages);
        System.out.println("  成功数: " + successCount.get());
        System.out.println("  失败数: " + failureCount.get());
        System.out.println("  成功率: " + String.format("%.2f%%",
            successCount.get() * 100.0 / totalMessages));
        System.out.println();

        System.out.println("性能:");
        System.out.println("  总耗时: " + testTotalTime + " ms");
        System.out.println("  平均TPS: " + String.format("%.2f",
            successCount.get() * 1000.0 / testTotalTime));
        System.out.println("  平均延迟: " + String.format("%.2f",
            totalElapsedMillis.get() * 1.0 / successCount.get()) + " ms");
        System.out.println();

        Map<String, ProxyMetricsSnapshot> metrics = client.getMetrics();
        for (Map.Entry<String, ProxyMetricsSnapshot> entry : metrics.entrySet()) {
            ProxyMetricsSnapshot snapshot = entry.getValue();
            System.out.println("代理: " + snapshot.getProxyAddr());
            System.out.println("  成功: " + snapshot.getSuccessCount());
            System.out.println("  失败: " + snapshot.getFailureCount());
            System.out.println("  成功率: " + String.format("%.2f%%",
                snapshot.getSuccessRate() * 100));
            System.out.println("  平均延迟: " + snapshot.getAvgElapsedMillis() + " ms");
        }

        client.shutdown();
        System.out.println("✓ Producer 已关闭");
    }
}