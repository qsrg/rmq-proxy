package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import com.mq.proxy.sdk.exception.ProxyConnectException;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.monitor.MetricsCollector;
import com.mq.proxy.sdk.remoting.ProxyRemotingClient;
import com.mq.proxy.sdk.trace.TraceCollector;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyClientFacade {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyClientFacade.class);
    
    private final ProxyClientConfig config;
    private final ProxyAddressManager addressManager;
    private final ProxyChannelManager channelManager;
    private final ProxyRemotingClient remotingClient;
    private final MetricsCollector metricsCollector;
    private final TraceCollector traceCollector;
    
    private final ScheduledExecutorService scheduledExecutor;
    
    private volatile boolean started = false;
    
    public ProxyClientFacade(ProxyClientConfig config) {
        this.config = config;
        this.addressManager = new ProxyAddressManager(config);
        this.remotingClient = new ProxyRemotingClient(config);
        this.channelManager = new ProxyChannelManager(config, remotingClient.getBootstrap());
        this.metricsCollector = new MetricsCollector(config);
        this.traceCollector = new TraceCollector(config);
        
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    }
    
    public void start() {
        if (!started) {
            startIdleChannelScan();
            started = true;
            log.info("ProxyClientFacade started");
        }
    }
    
    public byte[] invokeSync(byte[] request, long timeoutMillis) throws ProxyException {
        
        int maxRetryTimes = config.getRetryTimes();
        Exception lastException = null;
        
        for (int i = 0; i < maxRetryTimes; i++) {
            String proxyAddr = addressManager.selectProxyAddr();
            if (proxyAddr == null) {
                throw new ProxyConnectException("No available proxy address");
            }
            
            long startTime = System.currentTimeMillis();
            
            try {
                Channel channel = channelManager.getOrCreateChannel(proxyAddr);
                
                byte[] response = remotingClient.invokeSync(channel, request, timeoutMillis);
                
                addressManager.clearFault(proxyAddr);
                
                long elapsed = System.currentTimeMillis() - startTime;
                metricsCollector.recordSuccess(proxyAddr, elapsed);
                
                return response;
                
            } catch (Exception e) {
                addressManager.markFault(proxyAddr);
                channelManager.closeChannel(proxyAddr);
                
                metricsCollector.recordFailure(proxyAddr, e);
                
                lastException = e;
                log.warn("Request to proxy {} failed, attempt {}/{}, error: {}", 
                    proxyAddr, i + 1, maxRetryTimes, e.getMessage());
            }
        }
        
        throw new ProxyException("All proxy addresses failed after " + maxRetryTimes + " attempts", 
            lastException);
    }
    
    private void startIdleChannelScan() {
        scheduledExecutor.scheduleAtFixedRate(() -> {
            try {
                channelManager.scanAndCloseIdleChannels(config.getIdleChannelTimeoutMillis());
            } catch (Exception e) {
                log.error("Scan idle channel error", e);
            }
        }, config.getIdleChannelScanIntervalMillis(), 
           config.getIdleChannelScanIntervalMillis(), 
           TimeUnit.MILLISECONDS);
    }
    
    public void shutdown() {
        started = false;
        scheduledExecutor.shutdown();
        channelManager.closeAllChannels();
        remotingClient.shutdown();
        traceCollector.shutdown();
        log.info("ProxyClientFacade shutdown");
    }
    
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    public TraceCollector getTraceCollector() {
        return traceCollector;
    }
}
