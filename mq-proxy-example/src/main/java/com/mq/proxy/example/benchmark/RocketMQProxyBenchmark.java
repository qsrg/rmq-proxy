package com.mq.proxy.example.benchmark;

import org.apache.commons.cli.ParseException;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RocketMQProxyBenchmark {

    private static final String PROP_BENCH_ID = "BENCH_ID";
    private static final String PROP_SEND_TIME = "BENCH_SEND_TIME";

    public static void main(String[] args) throws Exception {
        BenchmarkOptions options;
        try {
            options = BenchmarkOptions.parse(args);
        } catch (ParseException | IllegalArgumentException e) {
            System.err.println("Invalid arguments: " + e.getMessage());
            BenchmarkOptions.printHelp();
            return;
        }
        if (options.isHelp()) {
            BenchmarkOptions.printHelp();
            return;
        }

        new RocketMQProxyBenchmark().run(options);
    }

    public void run(final BenchmarkOptions options) throws Exception {
        final BenchmarkMetrics metrics = new BenchmarkMetrics();
        final Set<String> consumedMessageIds = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean recording = new AtomicBoolean(false);
        final AtomicLong recordStartMillisRef = new AtomicLong(Long.MAX_VALUE);

        DefaultMQProducer producer = null;
        DefaultMQPushConsumer consumer = null;
        ExecutorService producerExecutor = null;
        Thread reporterThread = null;
        long recordStartMillis;

        try {
            if (options.getMode() == BenchmarkOptions.Mode.CONSUME || options.getMode() == BenchmarkOptions.Mode.MIXED) {
                consumer = startConsumer(options, metrics, consumedMessageIds, recording, recordStartMillisRef);
            }

            if (options.getMode() == BenchmarkOptions.Mode.PRODUCE || options.getMode() == BenchmarkOptions.Mode.MIXED) {
                producer = startProducer(options);
                producerExecutor = Executors.newFixedThreadPool(options.getProducerThreads(),
                        new NamedThreadFactory("BenchmarkProducer_"));
                startProducerWorkers(options, producer, producerExecutor, metrics, running, recording);
            }

            System.out.println("Benchmark started: target=" + options.getTarget()
                    + ", mode=" + options.getMode()
                    + ", namesrvAddr=" + options.resolveClientNamesrvAddr()
                    + ", topic=" + options.getTopic()
                    + ", warmupSeconds=" + options.getWarmupSeconds()
                    + ", durationSeconds=" + options.getDurationSeconds());

            if (options.getWarmupSeconds() > 0) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(options.getWarmupSeconds()));
            }
            metrics.reset();
            consumedMessageIds.clear();
            recordStartMillis = System.currentTimeMillis();
            recordStartMillisRef.set(recordStartMillis);
            recording.set(true);

            reporterThread = startReporter(options, metrics, running, recordStartMillis);
            Thread.sleep(TimeUnit.SECONDS.toMillis(options.getDurationSeconds()));
        } finally {
            running.set(false);
            recording.set(false);
            if (producerExecutor != null) {
                producerExecutor.shutdown();
                producerExecutor.awaitTermination(10, TimeUnit.SECONDS);
            }
            if (reporterThread != null) {
                reporterThread.interrupt();
                reporterThread.join(TimeUnit.SECONDS.toMillis(2));
            }
            if (producer != null) {
                producer.shutdown();
            }
            if (consumer != null) {
                consumer.shutdown();
            }
        }

        double elapsedSeconds = Math.max(0.001d,
                (System.currentTimeMillis() - recordStartMillis) / 1000.0d);
        BenchmarkMetrics.Snapshot snapshot = metrics.snapshot(elapsedSeconds);
        String json = BenchmarkReportFormatter.toJson(options, snapshot, elapsedSeconds);
        writeOutput(options.getOutput(), json);
        System.out.println(BenchmarkReportFormatter.toConsoleLine(snapshot, elapsedSeconds));
        System.out.println("Final result written to " + options.getOutput());
    }

    private DefaultMQProducer startProducer(BenchmarkOptions options) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer(options.getProducerGroup());
        producer.setNamesrvAddr(options.resolveClientNamesrvAddr());
        producer.setInstanceName(options.getInstanceId() + "-producer");
        producer.setSendMsgTimeout(options.getSendTimeoutMillis());
        producer.start();
        return producer;
    }

    private DefaultMQPushConsumer startConsumer(final BenchmarkOptions options, final BenchmarkMetrics metrics,
                                               final Set<String> consumedMessageIds,
                                               final AtomicBoolean recording,
                                               final AtomicLong recordStartMillisRef) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(options.getConsumerGroup());
        consumer.setNamesrvAddr(options.resolveClientNamesrvAddr());
        consumer.setInstanceName(options.getInstanceId() + "-consumer");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeThreadMin(options.getConsumerThreads());
        consumer.setConsumeThreadMax(options.getConsumerThreads());
        consumer.subscribe(options.getTopic(), "*");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                long startNanos = System.nanoTime();
                try {
                    long now = System.currentTimeMillis();
                    for (MessageExt msg : msgs) {
                        if (!recording.get()) {
                            continue;
                        }
                        String sendTimeProperty = msg.getUserProperty(PROP_SEND_TIME);
                        long sendTime = parseLong(sendTimeProperty, msg.getBornTimestamp());
                        if (sendTimeProperty != null && sendTime < recordStartMillisRef.get()) {
                            continue;
                        }
                        String benchId = msg.getUserProperty(PROP_BENCH_ID);
                        if (benchId != null && !consumedMessageIds.add(benchId)) {
                            metrics.recordDuplicate();
                        }
                        long latencyMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                        metrics.recordConsumeSuccess(latencyMillis, Math.max(0L, now - sendTime));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Throwable e) {
                    if (recording.get()) {
                        metrics.recordConsumeFailure();
                    }
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });
        consumer.start();
        return consumer;
    }

    private void startProducerWorkers(final BenchmarkOptions options, final DefaultMQProducer producer,
                                      ExecutorService producerExecutor, final BenchmarkMetrics metrics,
                                      final AtomicBoolean running, final AtomicBoolean recording) {
        final AtomicLong sequence = new AtomicLong();
        for (int i = 0; i < options.getProducerThreads(); i++) {
            final int workerId = i;
            producerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    while (running.get()) {
                        long sendStart = System.nanoTime();
                        try {
                            long seq = sequence.incrementAndGet();
                            String benchId = options.getInstanceId() + "-" + workerId + "-" + seq;
                            long sendTime = System.currentTimeMillis();
                            Message message = new Message(options.getTopic(), options.getTag(), benchId,
                                    buildBody(options.getMessageSize(), benchId));
                            message.putUserProperty(PROP_BENCH_ID, benchId);
                            message.putUserProperty(PROP_SEND_TIME, String.valueOf(sendTime));
                            SendResult result = producer.send(message);
                            if (!recording.get()) {
                                continue;
                            }
                            long latencyMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sendStart);
                            if (result != null && result.getSendStatus() == SendStatus.SEND_OK) {
                                metrics.recordSendSuccess(latencyMillis);
                            } else {
                                metrics.recordSendFailure(latencyMillis);
                            }
                        } catch (Throwable e) {
                            if (recording.get()) {
                                metrics.recordSendFailure(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sendStart));
                            }
                        }
                    }
                }
            });
        }
    }

    private Thread startReporter(final BenchmarkOptions options, final BenchmarkMetrics metrics,
                                 final AtomicBoolean running, final long recordStartMillis) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(options.getReportIntervalSeconds()));
                    } catch (InterruptedException e) {
                        return;
                    }
                    double elapsedSeconds = Math.max(0.001d,
                            (System.currentTimeMillis() - recordStartMillis) / 1000.0d);
                    System.out.println(BenchmarkReportFormatter.toConsoleLine(
                            metrics.snapshot(elapsedSeconds), elapsedSeconds));
                }
            }
        }, "BenchmarkReporter");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static byte[] buildBody(int messageSize, String benchId) {
        byte[] body = new byte[messageSize];
        byte[] prefix = benchId.getBytes(StandardCharsets.UTF_8);
        int length = Math.min(prefix.length, body.length);
        System.arraycopy(prefix, 0, body, 0, length);
        for (int i = length; i < body.length; i++) {
            body[i] = (byte) ('a' + (i % 26));
        }
        return body;
    }

    private static long parseLong(String value, long defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static void writeOutput(String output, String content) throws Exception {
        File file = new File(output);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("failed to create output directory: " + parent);
        }
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicLong index = new AtomicLong();

        NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + index.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }
}
