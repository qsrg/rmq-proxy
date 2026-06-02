package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.config.ProxyCommonConfig;
import com.mq.proxy.sdk.exception.ProxyConnectException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ProxyChannelManager {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyChannelManager.class);
    
    private final ProxyCommonConfig config;
    private final Bootstrap bootstrap;

    public static class ProbeResult {
        private final boolean active;
        private final boolean tlsReady;

        public ProbeResult(boolean active, boolean tlsReady) {
            this.active = active;
            this.tlsReady = tlsReady;
        }

        public boolean isActive() {
            return active;
        }

        public boolean isTlsReady() {
            return tlsReady;
        }
    }
    
    private final ConcurrentHashMap<String, ChannelWrapper> channelTable = new ConcurrentHashMap<>();
    
    private static class ChannelWrapper {
        private final Channel channel;
        private final AtomicLong lastUseTime = new AtomicLong(System.currentTimeMillis());
        
        public ChannelWrapper(Channel channel) {
            this.channel = channel;
        }
        
        public Channel getChannel() {
            return channel;
        }
        
        public long getLastUseTime() {
            return lastUseTime.get();
        }
        
        public void updateLastUseTime() {
            lastUseTime.set(System.currentTimeMillis());
        }
        
        public boolean isOK() {
            return channel != null && channel.isActive();
        }

        public boolean isTlsHandshakeComplete() {
            io.netty.handler.ssl.SslHandler sslHandler = channel.pipeline().get(io.netty.handler.ssl.SslHandler.class);
            if (sslHandler == null) {
                return true;
            }
            try {
                return sslHandler.handshakeFuture().isDone() && sslHandler.handshakeFuture().isSuccess();
            } catch (Exception e) {
                return false;
            }
        }
    }
    
    public ProxyChannelManager(ProxyCommonConfig config, Bootstrap bootstrap) {
        this.config = config;
        this.bootstrap = bootstrap;
    }
    
    public Channel getOrCreateChannel(String addr) throws ProxyConnectException {
        return getOrCreateChannel(addr, config.getConnectTimeoutMillis());
    }

    public Channel getOrCreateChannel(String addr, long timeoutMillis) throws ProxyConnectException {
        ChannelWrapper cw = channelTable.get(addr);
        if (cw != null && cw.isOK()) {
            cw.updateLastUseTime();
            return cw.getChannel();
        }

        return createChannel(addr, timeoutMillis);
    }

    private Channel createChannel(String addr, long timeoutMillis) throws ProxyConnectException {
        String[] parts = addr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        try {
            ChannelFuture future = bootstrap.connect(host, port);

            long effectiveTimeout = Math.max(timeoutMillis, 1L);
            boolean success = future.awaitUninterruptibly(effectiveTimeout);

            if (success && future.isSuccess()) {
                Channel channel = future.channel();
                ChannelWrapper cw = new ChannelWrapper(channel);
                channelTable.put(addr, cw);
                log.info("Created channel to proxy: {}", addr);
                return channel;
            }

            if (!success) {
                future.cancel(true);
                throw new ProxyConnectException(addr, "Connect timed out after " + effectiveTimeout + "ms");
            }

            Throwable cause = future.cause();
            throw new ProxyConnectException(addr, "Connect failed: " + (cause != null ? cause.getMessage() : "unknown"));

        } catch (Exception e) {
            if (e instanceof ProxyConnectException) {
                throw e;
            }
            throw new ProxyConnectException(addr, e);
        }
    }
    
    public void closeChannel(String addr) {
        ChannelWrapper cw = channelTable.remove(addr);
        if (cw != null && cw.getChannel() != null) {
            cw.getChannel().close();
            log.info("Closed channel to proxy: {}", addr);
        }
    }
    
    public void scanAndCloseIdleChannels(long timeoutMillis) {
        long now = System.currentTimeMillis();
        
        for (Map.Entry<String, ChannelWrapper> entry : channelTable.entrySet()) {
            String addr = entry.getKey();
            ChannelWrapper cw = entry.getValue();
            
            if (now - cw.getLastUseTime() > timeoutMillis) {
                closeChannel(addr);
                log.info("Closed idle channel: {}, idle time: {}ms", 
                    addr, now - cw.getLastUseTime());
            }
        }
    }
    
    public void closeAllChannels() {
        for (String addr : channelTable.keySet()) {
            closeChannel(addr);
        }
    }

    public boolean isChannelActive(String addr) {
        return probe(addr, config.getConnectTimeoutMillis(), false).isActive();
    }

    public boolean isChannelTlsReady(String addr) {
        return probe(addr, config.getConnectTimeoutMillis(), true).isTlsReady();
    }

    public ProbeResult probe(String addr, long timeoutMillis, boolean requireTlsReady) {
        ChannelWrapper cw = channelTable.get(addr);
        if (cw != null && cw.isOK()) {
            boolean tlsReady = !requireTlsReady || cw.isTlsHandshakeComplete();
            return new ProbeResult(true, tlsReady);
        }
        try {
            String[] parts = addr.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            ChannelFuture future = bootstrap.connect(host, port);
            long effectiveTimeout = Math.max(timeoutMillis, 1L);
            boolean success = future.awaitUninterruptibly(effectiveTimeout);
            if (success && future.isSuccess()) {
                Channel channel = future.channel();
                boolean tlsReady = !requireTlsReady || waitForTlsHandshake(channel, effectiveTimeout);
                channel.close();
                return new ProbeResult(true, tlsReady);
            }
        } catch (Exception ignored) {
        }
        return new ProbeResult(false, false);
    }

    private boolean isTlsHandshakeComplete(Channel channel) {
        io.netty.handler.ssl.SslHandler sslHandler = channel.pipeline().get(io.netty.handler.ssl.SslHandler.class);
        if (sslHandler == null) {
            return true;
        }
        try {
            return sslHandler.handshakeFuture().isDone() && sslHandler.handshakeFuture().isSuccess();
        } catch (Exception e) {
            return false;
        }
    }

    private boolean waitForTlsHandshake(Channel channel, long timeoutMillis) {
        io.netty.handler.ssl.SslHandler sslHandler = channel.pipeline().get(io.netty.handler.ssl.SslHandler.class);
        if (sslHandler == null) {
            return true;
        }
        try {
            return sslHandler.handshakeFuture().awaitUninterruptibly(Math.max(timeoutMillis, 1L), TimeUnit.MILLISECONDS)
                    && sslHandler.handshakeFuture().isSuccess();
        } catch (Exception e) {
            return false;
        }
    }
}
