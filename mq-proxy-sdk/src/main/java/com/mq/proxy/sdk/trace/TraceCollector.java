package com.mq.proxy.sdk.trace;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class TraceCollector {
    
    private static final Logger log = LoggerFactory.getLogger(TraceCollector.class);
    
    private final ProxyClientConfig config;
    
    private final AtomicLong traceIdSequence = new AtomicLong(0);
    
    private final BlockingQueue<TraceRecord> traceQueue = new LinkedBlockingQueue<>(10000);
    
    private final ExecutorService traceExecutor = Executors.newSingleThreadExecutor();
    
    public TraceCollector(ProxyClientConfig config) {
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
        
        if (!traceQueue.offer(record)) {
            log.warn("Trace queue is full, drop trace: {}", traceId);
        }
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
