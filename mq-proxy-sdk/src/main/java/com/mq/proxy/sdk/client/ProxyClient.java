package com.mq.proxy.sdk.client;

import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProxyClient {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyClient.class);
    
    private final ProxyClientConfig config;
    private final ProxyClientFacade facade;
    
    private volatile boolean started = false;
    
    public ProxyClient(ProxyClientConfig config) {
        this.config = config;
        this.facade = new ProxyClientFacade(config);
    }
    
    public void start() {
        if (!started) {
            facade.start();
            started = true;
            log.info("ProxyClient started, proxy addresses: {}", config.getProxyAddrs());
        }
    }
    
    public SendResult send(String topic, String tags, byte[] body) throws ProxyException {
        return send(topic, tags, null, body);
    }
    
    public SendResult send(String topic, String tags, String keys, byte[] body) 
            throws ProxyException {
        
        String traceId = null;
        if (config.isEnableTrace()) {
            traceId = facade.getTraceCollector().generateTraceId();
        }
        
        long startTime = System.currentTimeMillis();
        
        try {
            byte[] request = buildSendMessageRequest(topic, tags, keys, body, traceId);
            
            byte[] response = facade.invokeSync(request, config.getRequestTimeoutMillis());
            
            SendResult result = parseSendResult(response);
            result.setTraceId(traceId);
            
            if (config.isEnableTrace()) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, result.getMsgId(), 
                    startTime, true, null);
            }
            
            return result;
            
        } catch (Exception e) {
            if (config.isEnableTrace() && traceId != null) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, null, 
                    startTime, false, e.getMessage());
            }
            
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Send message failed", e);
        }
    }
    
    public PullResult pull(String topic, String consumerGroup, 
                          int queueId, long offset, int maxNums) throws ProxyException {
        
        try {
            byte[] request = buildPullMessageRequest(topic, consumerGroup, queueId, offset, maxNums);
            
            byte[] response = facade.invokeSync(request, config.getRequestTimeoutMillis());
            
            return parsePullResult(response);
            
        } catch (Exception e) {
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Pull message failed", e);
        }
    }
    
    public void shutdown() {
        if (started) {
            facade.shutdown();
            started = false;
            log.info("ProxyClient shutdown");
        }
    }
    
    public Map<String, ProxyMetricsSnapshot> getMetrics() {
        return facade.getMetricsCollector().getSnapshot();
    }
    
    private byte[] buildSendMessageRequest(String topic, String tags, String keys, 
                                          byte[] body, String traceId) {
        return ("SEND:" + topic + ":" + tags + ":" + keys + ":" + traceId + ":" + body.length)
            .getBytes();
    }
    
    private SendResult parseSendResult(byte[] response) {
        SendResult result = new SendResult();
        String str = new String(response);
        
        if (str.startsWith("OK:")) {
            result.setSuccess(true);
            String[] parts = str.substring(3).split(":");
            if (parts.length >= 1) {
                result.setMsgId(parts[0]);
            }
        } else {
            result.setSuccess(false);
            result.setErrorMsg(str);
        }
        
        return result;
    }
    
    private byte[] buildPullMessageRequest(String topic, String consumerGroup, 
                                          int queueId, long offset, int maxNums) {
        return ("PULL:" + topic + ":" + consumerGroup + ":" + queueId + ":" + offset + ":" + maxNums)
            .getBytes();
    }
    
    private PullResult parsePullResult(byte[] response) {
        PullResult result = new PullResult();
        return result;
    }
}
