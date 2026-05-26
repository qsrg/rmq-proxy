package com.mq.proxy.example.producer;

import com.mq.proxy.sdk.client.ProxyClient;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.client.SendResult;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;

import java.util.Map;

/**
 * 多代理地址与故障转移示例
 *
 * 展示如何配置多个代理地址实现高可用和故障转移
 */
public class MultiProxyProducer {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("  多代理地址与故障转移示例");
        System.out.println("========================================");
        System.out.println();

        // 配置多个代理地址（用分号分隔）
        ProxyClientConfig config = new ProxyClientConfig();

        // 多个代理地址配置
        config.setProxyAddrs("127.0.0.1:11911;127.0.0.1:11912;127.0.0.1:11913");
        System.out.println("代理地址配置:");
        System.out.println("  - " + config.getProxyAddrs().replace(";", "\n  - "));
        System.out.println();

        config.setProducerGroup("MultiProxyProducerGroup");

        // 重试配置
        config.setRetryTimes(3);  // 每个请求最多重试3次
        config.setFaultIsolationDurationMillis(30000);  // 故障隔离30秒
        System.out.println("重试配置:");
        System.out.println("  - 重试次数: " + config.getRetryTimes());
        System.out.println("  - 故障隔离时间: " + config.getFaultIsolationDurationMillis() + " ms");
        System.out.println("  - 请求超时: " + config.getRequestTimeoutMillis() + " ms");
        System.out.println();

        config.setEnableMetrics(true);  // 启用监控，观察故障转移效果

        ProxyClient client = new ProxyClient(config);

        try {
            client.start();
            System.out.println("✓ Producer 启动成功");
            System.out.println();
            System.out.println("SDK内部机制:");
            System.out.println("  1. Round-Robin轮询选择代理地址");
            System.out.println("  2. 失败的地址会被标记并隔离30秒");
            System.out.println("  3. 自动切换到下一个可用地址");
            System.out.println("  4. 隔离超时后地址自动恢复");
            System.out.println();

            String topic = "MultiProxyTopic";
            String tags = "TagA";

            // 场景1：正常发送（轮询到不同地址）
            System.out.println("【场景1】正常发送 - 观察轮询机制");
            System.out.println();

            for (int i = 0; i < 6; i++) {
                String body = "RoundRobin Message #" + i;
                String key = "RRKey_" + i;

                SendResult result = client.send(topic, tags, key, body.getBytes());

                System.out.printf("[%02d] 成功=%s, msgId=%s%n",
                    i, result.isSuccess(), result.getMsgId());

                Thread.sleep(100);  // 模拟业务间隔
            }
            System.out.println();

            // 场景2：模拟故障（停掉部分代理）
            System.out.println("【场景2】模拟代理故障");
            System.out.println();
            System.out.println("提示：可以手动停掉某个代理进程来观察故障转移");
            System.out.println("例如：停止 127.0.0.1:11911 代理");
            System.out.println();

            System.out.println("继续发送消息，SDK会自动跳过故障地址...");
            System.out.println();

            for (int i = 0; i < 10; i++) {
                String body = "FaultTolerance Message #" + i;
                String key = "FTKey_" + i;

                try {
                    SendResult result = client.send(topic, tags, key, body.getBytes());

                    if (result.isSuccess()) {
                        System.out.printf("[%02d] ✓ 发送成功 - msgId=%s%n", i, result.getMsgId());
                    } else {
                        System.out.printf("[%02d] ✗ 发送失败 - error=%s%n", i, result.getErrorMsg());
                    }
                } catch (ProxyException e) {
                    System.out.printf("[%02d] ✗ 异常 - %s%n", i, e.getMessage());
                }

                Thread.sleep(200);
            }
            System.out.println();

            // 查看监控数据
            System.out.println("【监控数据】查看故障转移统计");
            System.out.println();

            Map<String, ProxyMetricsSnapshot> metrics = client.getMetrics();
            for (Map.Entry<String, ProxyMetricsSnapshot> entry : metrics.entrySet()) {
                ProxyMetricsSnapshot snapshot = entry.getValue();
                System.out.println("代理: " + snapshot.getProxyAddr());
                System.out.println("  成功: " + snapshot.getSuccessCount());
                System.out.println("  失败: " + snapshot.getFailureCount());
                System.out.println("  成功率: " + String.format("%.2f%%", snapshot.getSuccessRate() * 100));
                System.out.println("  平均延迟: " + snapshot.getAvgElapsedMillis() + " ms");

                // 故障代理会显示较高的失败率
                if (snapshot.getFailureCount() > 0) {
                    System.out.println("  ⚠ 此代理有失败记录，可能被隔离");
                }
                System.out.println();
            }

            System.out.println("========================================");
            System.out.println("高可用最佳实践:");
            System.out.println("1. 配置至少2-3个代理地址");
            System.out.println("2. 合理设置重试次数（建议3-5次）");
            System.out.println("3. 监控失败率，及时发现问题");
            System.out.println("4. 故障隔离时间根据业务调整");
            System.out.println("5. 定期检查代理健康状态");
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("✗ 发生异常: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println("✓ Producer 已关闭");
        }
    }
}