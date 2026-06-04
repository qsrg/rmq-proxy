package com.mq.proxy.sdk.trace;

import com.mq.proxy.sdk.config.ProxyCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TraceCollector {
    
    private static final Logger log = LoggerFactory.getLogger(TraceCollector.class);
    
    private final ProxyCommonConfig config;
    
    private final AtomicLong traceIdSequence = new AtomicLong(0);
    
    private final BlockingQueue<TraceRecord> traceQueue = new LinkedBlockingQueue<>(10000);
    
    private final ExecutorService traceExecutor = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(10000), new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "TraceThread_" + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });
    
    private final AtomicLong recordedCount = new AtomicLong(0);
    
    public TraceCollector(ProxyCommonConfig config) {
        this.config = config;
        startTraceWriter();
    }
    
    public String generateTraceId() {
        return TraceIdGenerator.generate();
    }
    
    public void recordSendTrace(String traceId, String topic, String msgId,
                               long sendTime, boolean success, String errorMsg) {
        if (!config.isEnableTrace() || traceId == null) {
            return;
        }
        
        TraceRecord record = new TraceRecord();
        record.setTraceId(traceId);
        record.setTopic(topic);
        record.setMsgId(msgId);
        record.setSendTime(sendTime);
        record.setSuccess(success);
        record.setErrorMsg(errorMsg);
        record.setRecordTime(System.currentTimeMillis());
        
        recordedCount.incrementAndGet();
        
        if (!traceQueue.offer(record)) {
            log.warn("Trace queue is full, drop trace: {}", traceId);
        }
    }
    
    public long getRecordedCount() {
        return recordedCount.get();
    }
    
    private void startTraceWriter() {
        traceExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TraceRecord record = traceQueue.take();
                    writeTrace(record);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Write trace error", e);
                }
            }
        });
    }
    
    private void writeTrace(TraceRecord record) {
        log.info("Trace: traceId={}, topic={}, msgId={}, success={}, elapsed={}ms",
            record.getTraceId(), record.getTopic(), record.getMsgId(),
            record.isSuccess(), record.getRecordTime() - record.getSendTime());
    }
    
    public void shutdown() {
        traceExecutor.shutdown();
    }
}
