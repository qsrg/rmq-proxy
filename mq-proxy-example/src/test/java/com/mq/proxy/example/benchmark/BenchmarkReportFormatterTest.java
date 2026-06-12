package com.mq.proxy.example.benchmark;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BenchmarkReportFormatterTest {

    @Test
    public void formatsFinalSnapshotAsJson() throws Exception {
        BenchmarkOptions options = BenchmarkOptions.parse(new String[] {
                "--target", "proxy",
                "--proxyAddrs", "127.0.0.1:10913",
                "--topic", "BenchmarkTopic",
                "--mode", "mixed",
                "--instanceId", "node-a"
        });
        BenchmarkMetrics metrics = new BenchmarkMetrics();
        metrics.recordSendSuccess(3);
        metrics.recordConsumeSuccess(7, 50);

        String json = BenchmarkReportFormatter.toJson(options, metrics.snapshot(2.0d), 2.0d);

        assertTrue(json.contains("\"target\":\"PROXY\""));
        assertTrue(json.contains("\"clientNamesrvAddr\":\"127.0.0.1:10913\""));
        assertTrue(json.contains("\"instanceId\":\"node-a\""));
        assertTrue(json.contains("\"sendSuccess\":1"));
        assertTrue(json.contains("\"consumeSuccess\":1"));
        assertTrue(json.contains("\"sendSuccessTps\":0.5"));
        assertTrue(json.contains("\"consumeLagMs\""));
    }
}
