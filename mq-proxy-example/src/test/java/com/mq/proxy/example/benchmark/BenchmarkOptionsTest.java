package com.mq.proxy.example.benchmark;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BenchmarkOptionsTest {

    @Test
    public void parsesProxyTargetOptions() throws Exception {
        BenchmarkOptions options = BenchmarkOptions.parse(new String[] {
                "--target", "proxy",
                "--proxyAddrs", "127.0.0.1:10913;127.0.0.2:10913",
                "--namesrvAddrs", "10.0.0.1:9876;10.0.0.2:9876",
                "--topic", "BenchmarkTopic",
                "--mode", "mixed",
                "--producerThreads", "12",
                "--consumerThreads", "6",
                "--messageSize", "2048",
                "--durationSeconds", "120",
                "--warmupSeconds", "10",
                "--reportIntervalSeconds", "5",
                "--output", "target/benchmark-result.json"
        });

        assertEquals(BenchmarkOptions.Target.PROXY, options.getTarget());
        assertEquals("127.0.0.1:10913;127.0.0.2:10913", options.resolveClientNamesrvAddr());
        assertEquals("10.0.0.1:9876;10.0.0.2:9876", options.getNamesrvAddrs());
        assertEquals(BenchmarkOptions.Mode.MIXED, options.getMode());
        assertEquals("BenchmarkTopic", options.getTopic());
        assertEquals(12, options.getProducerThreads());
        assertEquals(6, options.getConsumerThreads());
        assertEquals(2048, options.getMessageSize());
        assertEquals(120, options.getDurationSeconds());
        assertEquals(10, options.getWarmupSeconds());
        assertEquals(5, options.getReportIntervalSeconds());
        assertEquals("target/benchmark-result.json", options.getOutput());
    }

    @Test
    public void parsesDirectTargetOptions() throws Exception {
        BenchmarkOptions options = BenchmarkOptions.parse(new String[] {
                "--target", "direct",
                "--namesrvAddrs", "10.0.0.1:9876",
                "--mode", "produce"
        });

        assertEquals(BenchmarkOptions.Target.DIRECT, options.getTarget());
        assertEquals("10.0.0.1:9876", options.resolveClientNamesrvAddr());
        assertEquals(BenchmarkOptions.Mode.PRODUCE, options.getMode());
    }

    @Test
    public void rejectsProxyTargetWithoutProxyAddrs() {
        try {
            BenchmarkOptions.parse(new String[] {
                    "--target", "proxy",
                    "--mode", "consume"
            });
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("proxyAddrs"));
            return;
        } catch (Exception e) {
            throw new AssertionError("unexpected exception", e);
        }
        throw new AssertionError("expected IllegalArgumentException");
    }
}
