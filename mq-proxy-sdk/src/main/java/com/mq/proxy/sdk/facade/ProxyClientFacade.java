package com.mq.proxy.sdk.facade;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.sdk.config.ProxyCommonConfig;
import com.mq.proxy.sdk.exception.ProxyConnectException;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.monitor.MetricsCollector;
import com.mq.proxy.sdk.remoting.InvokeCallback;
import com.mq.proxy.sdk.remoting.ProxyRemotingClient;
import com.mq.proxy.sdk.trace.TraceCollector;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyClientFacade {

    private static final Logger log = LoggerFactory.getLogger(ProxyClientFacade.class);

    private final ProxyCommonConfig config;
    private final ProxyAddressManager addressManager;
    private final ProxyChannelManager channelManager;
    private final ProxyRemotingClient remotingClient;
    private final MetricsCollector metricsCollector;
    private final TraceCollector traceCollector;

    private final ScheduledExecutorService scheduledExecutor;

    private volatile boolean started = false;

    public ProxyClientFacade(ProxyCommonConfig config) {
        this.config = config;
        this.addressManager = new ProxyAddressManager(config);
        this.remotingClient = new ProxyRemotingClient(config);
        this.channelManager = new ProxyChannelManager(config, remotingClient.getBootstrap());
        this.metricsCollector = new MetricsCollector(config);
        this.traceCollector = new TraceCollector(config);

        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "FacadeScheduledThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void start() {
        if (!started) {
            remotingClient.start();
            startIdleChannelScan();
            addressManager.startDetector(channelManager);
            started = true;
            log.info("ProxyClientFacade started");
        }
    }

    public RemotingCommand invokeSync(RemotingCommand request, long timeoutMillis)
            throws ProxyException {

        int maxRetryTimes = config.getRetryTimes();
        Exception lastException = null;
        long beginTimestamp = System.currentTimeMillis();
        long deadline = beginTimestamp + timeoutMillis;

        for (int i = 0; i < maxRetryTimes; i++) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }

            String proxyAddr = addressManager.selectProxyAddr();
            if (proxyAddr == null) {
                throw new ProxyConnectException("No available proxy address");
            }

            long startTime = System.currentTimeMillis();

            try {
                long connectTimeout = Math.min(remaining, config.getConnectTimeoutMillis());
                Channel channel = channelManager.getOrCreateChannel(proxyAddr, connectTimeout);

                long afterConnectRemaining = deadline - System.currentTimeMillis();
                if (afterConnectRemaining <= 0) {
                    throw new ProxyConnectException(proxyAddr, "No time left after connect");
                }

                long curTimeout = computeTimeoutForRetry(afterConnectRemaining, i, maxRetryTimes);

                RemotingCommand response = remotingClient.invokeSync(channel, request, curTimeout);

                long elapsed = System.currentTimeMillis() - startTime;
                addressManager.clearFault(proxyAddr);
                metricsCollector.recordSuccess(proxyAddr, elapsed);

                return response;

            } catch (Exception e) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (!isRequestTimeoutException(e)) {
                    long faultLatency = elapsed;
                    if (isConnectionException(e)) {
                        faultLatency = 10000L;
                    }
                    addressManager.markFault(proxyAddr, faultLatency);
                    channelManager.closeChannel(proxyAddr);
                }

                metricsCollector.recordFailure(proxyAddr, e);

                lastException = e;
                log.warn("Request to proxy {} failed, attempt {}/{}, latency={}ms, error: {}",
                    proxyAddr, i + 1, maxRetryTimes, elapsed, e.getMessage());
            }
        }

        throw new ProxyException("All proxy addresses failed after " + maxRetryTimes + " attempts",
            lastException);
    }

    public void invokeAsync(RemotingCommand request, long timeoutMillis, InvokeCallback callback)
            throws ProxyException {

        String proxyAddr = addressManager.selectProxyAddr();
        if (proxyAddr == null) {
            throw new ProxyConnectException("No available proxy address");
        }

        long startTime = System.currentTimeMillis();

        try {
            Channel channel = channelManager.getOrCreateChannel(proxyAddr);
            InvokeCallback wrappedCallback = new InvokeCallback() {
                @Override
                public void onSuccess(com.mq.proxy.core.protocol.RemotingCommand response) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    addressManager.clearFault(proxyAddr);
                    metricsCollector.recordSuccess(proxyAddr, elapsed);
                    callback.onSuccess(response);
                }

                @Override
                public void onException(Throwable cause) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    long faultLatency = elapsed;
                    if (isConnectionException(cause)) {
                        faultLatency = 10000L;
                    }
                    addressManager.markFault(proxyAddr, faultLatency);
                    channelManager.closeChannel(proxyAddr);
                    metricsCollector.recordFailure(proxyAddr,
                            cause instanceof Exception ? (Exception) cause : new RuntimeException(cause));
                    callback.onException(cause);
                }
            };
            remotingClient.invokeAsync(channel, request, timeoutMillis, wrappedCallback);
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            long faultLatency = elapsed;
            if (isConnectionException(e)) {
                faultLatency = 10000L;
            }
            addressManager.markFault(proxyAddr, faultLatency);
            channelManager.closeChannel(proxyAddr);
            metricsCollector.recordFailure(proxyAddr, e);
            throw new ProxyException("invokeAsync failed for proxy " + proxyAddr, e);
        }
    }

    public void invokeOneway(RemotingCommand request) throws ProxyException {
        String proxyAddr = addressManager.selectProxyAddr();
        if (proxyAddr == null) {
            throw new ProxyConnectException("No available proxy address");
        }

        long startTime = System.currentTimeMillis();
        try {
            Channel channel = channelManager.getOrCreateChannel(proxyAddr);
            remotingClient.invokeOneway(channel, request);
        } catch (Exception e) {
            long elapsed = System.currentTimeMillis() - startTime;
            long faultLatency = elapsed;
            if (isConnectionException(e)) {
                faultLatency = 10000L;
            }
            addressManager.markFault(proxyAddr, faultLatency);
            channelManager.closeChannel(proxyAddr);
            throw new ProxyException("invokeOneway failed for proxy " + proxyAddr, e);
        }
    }

    private long computeTimeoutForRetry(long remaining, int currentAttempt, int maxRetryTimes) {
        long maxPerRetry = config.getRequestTimeoutPerRetryMillis();
        if (maxPerRetry > 0 && currentAttempt < maxRetryTimes - 1 && remaining > maxPerRetry) {
            return maxPerRetry;
        }
        return remaining;
    }

    private boolean isConnectionException(Throwable e) {
        Throwable current = e;
        while (current != null) {
            if (current instanceof ProxyConnectException) {
                return true;
            }
            if (matchConnectionMessage(current.getMessage())) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean isRequestTimeoutException(Throwable e) {
        Throwable current = e;
        while (current != null) {
            String msg = current.getMessage();
            if (msg != null && msg.toLowerCase().contains("request timeout")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean matchConnectionMessage(String msg) {
        if (msg == null) {
            return false;
        }
        String lower = msg.toLowerCase();
        return lower.contains("connection refused")
                || lower.contains("connection reset")
                || lower.contains("broken pipe")
                || lower.contains("connect timed out")
                || lower.contains("no route to host")
                || lower.contains("network is unreachable");
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
        addressManager.shutdownDetector();
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

    public ProxyRemotingClient getRemotingClient() {
        return remotingClient;
    }
}
