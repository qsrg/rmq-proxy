package com.mq.proxy.example.benchmark;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

public class BenchmarkMetrics {

    private final LongAdder sendSuccess = new LongAdder();
    private final LongAdder sendFailure = new LongAdder();
    private final LongAdder consumeSuccess = new LongAdder();
    private final LongAdder consumeFailure = new LongAdder();
    private final LongAdder duplicateMessages = new LongAdder();
    private final Histogram sendLatency = new Histogram();
    private final Histogram consumeLatency = new Histogram();
    private final Histogram consumeLag = new Histogram();

    public void recordSendSuccess(long latencyMillis) {
        sendSuccess.increment();
        sendLatency.record(latencyMillis);
    }

    public void recordSendFailure(long latencyMillis) {
        sendFailure.increment();
    }

    public void recordConsumeSuccess(long latencyMillis, long lagMillis) {
        consumeSuccess.increment();
        consumeLatency.record(latencyMillis);
        consumeLag.record(lagMillis);
    }

    public void recordConsumeFailure() {
        consumeFailure.increment();
    }

    public void recordDuplicate() {
        duplicateMessages.increment();
    }

    public void reset() {
        sendSuccess.reset();
        sendFailure.reset();
        consumeSuccess.reset();
        consumeFailure.reset();
        duplicateMessages.reset();
        sendLatency.reset();
        consumeLatency.reset();
        consumeLag.reset();
    }

    public Snapshot snapshot(double elapsedSeconds) {
        long sendSuccessValue = sendSuccess.sum();
        long sendFailureValue = sendFailure.sum();
        long consumeSuccessValue = consumeSuccess.sum();
        long consumeFailureValue = consumeFailure.sum();
        return new Snapshot(
                sendSuccessValue,
                sendFailureValue,
                consumeSuccessValue,
                consumeFailureValue,
                duplicateMessages.sum(),
                tps(sendSuccessValue, elapsedSeconds),
                tps(consumeSuccessValue, elapsedSeconds),
                errorRate(sendFailureValue, sendSuccessValue + sendFailureValue),
                errorRate(consumeFailureValue, consumeSuccessValue + consumeFailureValue),
                sendLatency.snapshot(),
                consumeLatency.snapshot(),
                consumeLag.snapshot());
    }

    private static double tps(long count, double elapsedSeconds) {
        return elapsedSeconds > 0 ? count / elapsedSeconds : 0.0d;
    }

    private static double errorRate(long failures, long total) {
        return total > 0 ? (double) failures / total : 0.0d;
    }

    private static class Histogram {
        private static final int MAX_BUCKET_MILLIS = 60000;

        private final AtomicLongArray buckets = new AtomicLongArray(MAX_BUCKET_MILLIS + 2);
        private final LongAdder count = new LongAdder();
        private final LongAdder sum = new LongAdder();
        private final AtomicLong max = new AtomicLong();

        void record(long millis) {
            long boundedMillis = Math.max(0L, millis);
            int bucket = boundedMillis > MAX_BUCKET_MILLIS ? MAX_BUCKET_MILLIS + 1 : (int) boundedMillis;
            buckets.incrementAndGet(bucket);
            count.increment();
            sum.add(boundedMillis);
            updateMax(boundedMillis);
        }

        Snapshot.LatencySnapshot snapshot() {
            long total = count.sum();
            if (total == 0L) {
                return new Snapshot.LatencySnapshot(0, 0, 0, 0, 0, 0, 0);
            }
            return new Snapshot.LatencySnapshot(
                    total,
                    sum.sum() / total,
                    percentile(total, 0.50d),
                    percentile(total, 0.90d),
                    percentile(total, 0.99d),
                    max.get(),
                    buckets.get(MAX_BUCKET_MILLIS + 1));
        }

        void reset() {
            for (int i = 0; i < buckets.length(); i++) {
                buckets.set(i, 0L);
            }
            count.reset();
            sum.reset();
            max.set(0L);
        }

        private long percentile(long total, double percentile) {
            long threshold = Math.max(1L, (long) Math.ceil(total * percentile));
            long seen = 0L;
            for (int i = 0; i < buckets.length(); i++) {
                seen += buckets.get(i);
                if (seen >= threshold) {
                    return i > MAX_BUCKET_MILLIS ? MAX_BUCKET_MILLIS + 1L : i;
                }
            }
            return 0L;
        }

        private void updateMax(long value) {
            long current;
            do {
                current = max.get();
                if (value <= current) {
                    return;
                }
            } while (!max.compareAndSet(current, value));
        }
    }

    public static class Snapshot {
        private final long sendSuccess;
        private final long sendFailure;
        private final long consumeSuccess;
        private final long consumeFailure;
        private final long duplicateMessages;
        private final double sendSuccessTps;
        private final double consumeSuccessTps;
        private final double sendErrorRate;
        private final double consumeErrorRate;
        private final LatencySnapshot sendLatency;
        private final LatencySnapshot consumeLatency;
        private final LatencySnapshot consumeLag;

        Snapshot(long sendSuccess, long sendFailure, long consumeSuccess, long consumeFailure,
                 long duplicateMessages, double sendSuccessTps, double consumeSuccessTps,
                 double sendErrorRate, double consumeErrorRate, LatencySnapshot sendLatency,
                 LatencySnapshot consumeLatency, LatencySnapshot consumeLag) {
            this.sendSuccess = sendSuccess;
            this.sendFailure = sendFailure;
            this.consumeSuccess = consumeSuccess;
            this.consumeFailure = consumeFailure;
            this.duplicateMessages = duplicateMessages;
            this.sendSuccessTps = sendSuccessTps;
            this.consumeSuccessTps = consumeSuccessTps;
            this.sendErrorRate = sendErrorRate;
            this.consumeErrorRate = consumeErrorRate;
            this.sendLatency = sendLatency;
            this.consumeLatency = consumeLatency;
            this.consumeLag = consumeLag;
        }

        public long getSendSuccess() {
            return sendSuccess;
        }

        public long getSendFailure() {
            return sendFailure;
        }

        public long getSendTotal() {
            return sendSuccess + sendFailure;
        }

        public long getConsumeSuccess() {
            return consumeSuccess;
        }

        public long getConsumeFailure() {
            return consumeFailure;
        }

        public long getConsumeTotal() {
            return consumeSuccess + consumeFailure;
        }

        public long getDuplicateMessages() {
            return duplicateMessages;
        }

        public double getSendSuccessTps() {
            return sendSuccessTps;
        }

        public double getConsumeSuccessTps() {
            return consumeSuccessTps;
        }

        public double getSendErrorRate() {
            return sendErrorRate;
        }

        public double getConsumeErrorRate() {
            return consumeErrorRate;
        }

        public LatencySnapshot getSendLatency() {
            return sendLatency;
        }

        public LatencySnapshot getConsumeLatency() {
            return consumeLatency;
        }

        public LatencySnapshot getConsumeLag() {
            return consumeLag;
        }

        public static class LatencySnapshot {
            private final long count;
            private final long avg;
            private final long p50;
            private final long p90;
            private final long p99;
            private final long max;
            private final long overflow;

            LatencySnapshot(long count, long avg, long p50, long p90, long p99, long max, long overflow) {
                this.count = count;
                this.avg = avg;
                this.p50 = p50;
                this.p90 = p90;
                this.p99 = p99;
                this.max = max;
                this.overflow = overflow;
            }

            public long getCount() {
                return count;
            }

            public long getAvg() {
                return avg;
            }

            public long getP50() {
                return p50;
            }

            public long getP90() {
                return p90;
            }

            public long getP99() {
                return p99;
            }

            public long getMax() {
                return max;
            }

            public long getOverflow() {
                return overflow;
            }
        }
    }
}
