package com.mq.proxy.example.benchmark;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BenchmarkMetricsTest {

    @Test
    public void recordsSendCountersAndLatencyPercentiles() {
        BenchmarkMetrics metrics = new BenchmarkMetrics();

        metrics.recordSendSuccess(1);
        metrics.recordSendSuccess(2);
        metrics.recordSendSuccess(3);
        metrics.recordSendFailure(10);

        BenchmarkMetrics.Snapshot snapshot = metrics.snapshot(2.0d);

        assertEquals(3L, snapshot.getSendSuccess());
        assertEquals(1L, snapshot.getSendFailure());
        assertEquals(4L, snapshot.getSendTotal());
        assertEquals(1.5d, snapshot.getSendSuccessTps(), 0.001d);
        assertEquals(0.25d, snapshot.getSendErrorRate(), 0.001d);
        assertEquals(2L, snapshot.getSendLatency().getAvg());
        assertEquals(2L, snapshot.getSendLatency().getP50());
        assertEquals(3L, snapshot.getSendLatency().getP99());
        assertEquals(3L, snapshot.getSendLatency().getMax());
    }

    @Test
    public void recordsConsumeCountersDuplicatesAndLag() {
        BenchmarkMetrics metrics = new BenchmarkMetrics();

        metrics.recordConsumeSuccess(5, 100);
        metrics.recordConsumeSuccess(15, 300);
        metrics.recordConsumeFailure();
        metrics.recordDuplicate();

        BenchmarkMetrics.Snapshot snapshot = metrics.snapshot(4.0d);

        assertEquals(2L, snapshot.getConsumeSuccess());
        assertEquals(1L, snapshot.getConsumeFailure());
        assertEquals(1L, snapshot.getDuplicateMessages());
        assertEquals(0.5d, snapshot.getConsumeSuccessTps(), 0.001d);
        assertEquals(10L, snapshot.getConsumeLatency().getAvg());
        assertEquals(15L, snapshot.getConsumeLatency().getP99());
        assertEquals(200L, snapshot.getConsumeLag().getAvg());
        assertEquals(300L, snapshot.getConsumeLag().getMax());
    }
}
