package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.codec.RemotingCommandDecoder;
import com.mq.proxy.core.protocol.codec.RemotingCommandEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class NettyRemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final NettyClientConfig nettyClientConfig;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private SslContext sslContext;
    private final ConcurrentHashMap<String, Channel> channelTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>();
    private final AtomicInteger opaqueCounter = new AtomicInteger(0);
    private final AtomicInteger addressSelector = new AtomicInteger(0);
    private ExecutorService callbackExecutor;
    private final ConcurrentHashMap<Integer, RemotingProcessor> processorTable = new ConcurrentHashMap<>();
    private final ReentrantLock createChannelLock = new ReentrantLock();

    public NettyRemotingClient(NettyClientConfig nettyClientConfig) {
        this.nettyClientConfig = nettyClientConfig;
        if (nettyClientConfig.isTlsEnabled()) {
            try {
                this.sslContext = buildSslContext();
                log.info("TLS client enabled, SSL context initialized");
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize client TLS SSL context", e);
            }
        }
    }

    private SslContext buildSslContext() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();

        if (nettyClientConfig.getTlsClientCertPath() != null && nettyClientConfig.getTlsClientKeyPath() != null) {
            InputStream certStream = null;
            InputStream keyStream = null;
            try {
                certStream = new FileInputStream(nettyClientConfig.getTlsClientCertPath());
                keyStream = new FileInputStream(nettyClientConfig.getTlsClientKeyPath());
                String clientKeyPassword = System.getProperty("proxy.tlsClientKeyPassword");
                builder.keyManager(certStream, keyStream, clientKeyPassword);
            } finally {
                if (certStream != null) {
                    certStream.close();
                }
                if (keyStream != null) {
                    keyStream.close();
                }
            }
        }

        if (nettyClientConfig.getTlsTrustCertPath() != null) {
            InputStream trustCertStream = null;
            try {
                trustCertStream = new FileInputStream(nettyClientConfig.getTlsTrustCertPath());
                builder.trustManager(trustCertStream);
            } finally {
                if (trustCertStream != null) {
                    trustCertStream.close();
                }
            }
        } else {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            log.warn("TLS enabled without trustCertPath, using InsecureTrustManagerFactory (not for production)");
        }

        return builder.build();
    }

    public void start() {
        this.eventLoopGroup = new NioEventLoopGroup(this.nettyClientConfig.getClientWorkerThreadNums());
        this.callbackExecutor = new ThreadPoolExecutor(
            nettyClientConfig.getClientCallbackExecutorThreads() > 0
                ? nettyClientConfig.getClientCallbackExecutorThreads()
                : 4,
            nettyClientConfig.getClientCallbackExecutorThreads() > 0
                ? nettyClientConfig.getClientCallbackExecutorThreads()
                : 4,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(10000),
            new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyClientCallbackThread_" + threadIndex.incrementAndGet());
                }
            });

        this.bootstrap = new Bootstrap();
        this.bootstrap.group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, this.nettyClientConfig.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        if (sslContext != null) {
                            SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
                            ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
                        }
                        ch.pipeline().addLast("idleStateHandler",
                                new IdleStateHandler(0, 0, nettyClientConfig.getClientChannelMaxIdleTimeSeconds()));
                        ch.pipeline().addLast(new RemotingCommandDecoder());
                        ch.pipeline().addLast(new RemotingCommandEncoder());
                        ch.pipeline().addLast(new NettyClientHandler());
                    }
                });
    }

    public void shutdown() {
        for (Map.Entry<String, Channel> entry : this.channelTable.entrySet()) {
            closeChannel(entry.getKey(), entry.getValue(), "shutdown");
        }
        this.channelTable.clear();
        if (this.eventLoopGroup != null) {
            this.eventLoopGroup.shutdownGracefully();
        }
        if (this.callbackExecutor != null) {
            this.callbackExecutor.shutdown();
        }
        for (ResponseFuture responseFuture : this.responseTable.values()) {
            responseFuture.putResponse(null);
        }
        this.responseTable.clear();
    }

    public Channel getAndCreateChannel(String addr) throws Exception {
        Channel channel = this.channelTable.get(addr);
        if (channel != null && channel.isActive()) {
            return channel;
        }

        try {
            if (!createChannelLock.tryLock(this.nettyClientConfig.getConnectTimeoutMillis(), TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("Failed to acquire channel creation lock within timeout, addr: " + addr);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for channel creation lock, addr: " + addr);
        }

        try {
            // double-check: lock may have been acquired after another thread already created the channel
            channel = this.channelTable.get(addr);
            if (channel != null && channel.isActive()) {
                return channel;
            }

            int separator = addr.lastIndexOf(':');
            if (separator <= 0 || separator == addr.length() - 1) {
                throw new IllegalArgumentException("Invalid remote address: " + addr);
            }
            String host = addr.substring(0, separator);
            int port = Integer.parseInt(addr.substring(separator + 1));
            ChannelFuture channelFuture = this.bootstrap.connect(host, port).sync();
            channel = channelFuture.channel();
            Channel oldChannel = this.channelTable.put(addr, channel);
            if (oldChannel != null && oldChannel != channel) {
                closeChannel(addr, oldChannel, "replaced");
            }
            log.info("NettyRemotingClient connected: addr={}, localAddress={}, remoteAddress={}",
                    addr, channel.localAddress(), channel.remoteAddress());
            return channel;
        } finally {
            createChannelLock.unlock();
        }
    }

    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        List<String> targetAddrs = parseTargetAddrs(addr);
        if (targetAddrs.isEmpty()) {
            throw new IllegalArgumentException("addr is blank");
        }
        if (targetAddrs.size() == 1) {
            return invokeSyncSingle(targetAddrs.get(0), request, timeoutMillis);
        }

        Exception lastException = null;
        long deadline = System.currentTimeMillis() + timeoutMillis;
        int startIndex = Math.floorMod(this.addressSelector.getAndIncrement(), targetAddrs.size());
        for (int i = 0; i < targetAddrs.size(); i++) {
            String targetAddr = targetAddrs.get((startIndex + i) % targetAddrs.size());
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            try {
                return invokeSyncSingle(targetAddr, request, remaining);
            } catch (Exception e) {
                lastException = e;
                log.warn("invokeSync failed for addr {}, trying next address if available: {}", targetAddr, e.getMessage());
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        throw new RuntimeException("invokeSync timeout, addr: " + addr + ", timeoutMillis: " + timeoutMillis);
    }

    protected RemotingCommand invokeSyncSingle(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        Channel channel = getAndCreateChannel(addr);
        if (channel == null || !channel.isActive()) {
            closeChannel(addr, channel, "inactive-before-sync");
            throw new RuntimeException("channel is not active, addr: " + addr);
        }
        request.setOpaque(this.opaqueCounter.getAndIncrement());
        ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), channel, timeoutMillis);
        this.responseTable.put(request.getOpaque(), responseFuture);
        try {
            ChannelFuture writeFuture = channel.writeAndFlush(request);
            if (writeFuture != null) {
                writeFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        if (future.isSuccess()) {
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        responseFuture.setSendRequestOK(false);
                        responseFuture.setCause(future.cause());
                        responseFuture.putResponse(null);
                    }
                });
            }
            RemotingCommand response = responseFuture.waitResponse(timeoutMillis);
            if (response == null) {
                if (!responseFuture.isSendRequestOK()) {
                    closeChannel(addr, channel, "send-failed");
                    throw new RuntimeException("send request failed, addr: " + addr, responseFuture.getCause());
                }
                throw new RuntimeException("invokeSync timeout, addr: " + addr + ", timeoutMillis: " + timeoutMillis);
            }
            return response;
        } catch (Exception e) {
            if (!(e instanceof RuntimeException && e.getMessage() != null
                    && (e.getMessage().startsWith("invokeSync timeout")
                    || e.getMessage().startsWith("send request failed")))) {
                closeChannel(addr, channel, "sync-failed");
            }
            throw e;
        } finally {
            this.responseTable.remove(request.getOpaque());
        }
    }

    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        List<String> targetAddrs = parseTargetAddrs(addr);
        if (targetAddrs.isEmpty()) {
            throw new IllegalArgumentException("addr is blank");
        }
        if (targetAddrs.size() == 1) {
            invokeOnewaySingle(targetAddrs.get(0), request, timeoutMillis);
            return;
        }

        Exception lastException = null;
        int startIndex = Math.floorMod(this.addressSelector.getAndIncrement(), targetAddrs.size());
        for (int i = 0; i < targetAddrs.size(); i++) {
            String targetAddr = targetAddrs.get((startIndex + i) % targetAddrs.size());
            try {
                invokeOnewaySingle(targetAddr, request, timeoutMillis);
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("invokeOneway failed for addr {}, trying next address if available: {}", targetAddr, e.getMessage());
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        throw new RuntimeException("invokeOneway failed, addr: " + addr);
    }

    protected void invokeOnewaySingle(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        Channel channel = getAndCreateChannel(addr);
        if (channel == null || !channel.isActive()) {
            closeChannel(addr, channel, "inactive-before-oneway");
            throw new RuntimeException("channel is not active, addr: " + addr);
        }
        request.markOnewayRPC();
        channel.writeAndFlush(request);
    }

    private void closeChannel(String addr, Channel channel, String reason) {
        if (channel == null) {
            this.channelTable.remove(addr);
            log.info("NettyRemotingClient disconnected: addr={}, reason={}, channel=null", addr, reason);
            return;
        }
        boolean removed = this.channelTable.remove(addr, channel);
        if (!removed) {
            Channel current = this.channelTable.get(addr);
            if (current == channel) {
                this.channelTable.remove(addr);
            }
        }
        log.info("NettyRemotingClient disconnected: addr={}, localAddress={}, remoteAddress={}, reason={}",
                addr, channel.localAddress(), channel.remoteAddress(), reason);
        try {
            channel.close();
        } catch (Exception e) {
            log.warn("Failed to close channel for addr {}: {}", addr, e.getMessage());
        }
    }

    private void removeChannel(Channel channel, String reason) {
        if (channel == null) {
            return;
        }
        for (String addr : this.channelTable.keySet()) {
            if (this.channelTable.remove(addr, channel)) {
                log.info("NettyRemotingClient disconnected: addr={}, localAddress={}, remoteAddress={}, reason={}",
                        addr, channel.localAddress(), channel.remoteAddress(), reason);
                return;
            }
        }
        log.debug("NettyRemotingClient disconnected: localAddress={}, remoteAddress={}, reason={}, addr=unknown",
                channel.localAddress(), channel.remoteAddress(), reason);
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.nettyClientConfig.setNamesrvAddr(namesrvAddr);
    }

    public String getNamesrvAddr() {
        return this.nettyClientConfig.getNamesrvAddr();
    }

    public void registerProcessor(int requestCode, RemotingProcessor processor) {
        this.processorTable.put(requestCode, processor);
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            if (msg.isResponseType()) {
                ResponseFuture responseFuture = responseTable.get(msg.getOpaque());
                if (responseFuture != null) {
                    responseFuture.putResponse(msg);
                    responseTable.remove(msg.getOpaque());
                }
            } else {
                RemotingProcessor processor = processorTable.get(msg.getCode());
                if (processor != null) {
                    if (callbackExecutor != null) {
                        final RemotingCommand finalMsg = msg;
                        final Channel channel = ctx.channel();
                        callbackExecutor.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    RemotingCommand response = processor.processRequest(channel, finalMsg);
                                    if (!finalMsg.isOnewayRPC() && response != null) {
                                        response.setOpaque(finalMsg.getOpaque());
                                        channel.writeAndFlush(response);
                                    }
                                } catch (Exception e) {
                                    log.error("processRequest exception", e);
                                }
                            }
                        });
                    } else {
                        RemotingCommand response = processor.processRequest(ctx.channel(), msg);
                        if (!msg.isOnewayRPC() && response != null) {
                            response.setOpaque(msg.getOpaque());
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            removeChannel(ctx.channel(), "remote-inactive");
            super.channelInactive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.ALL_IDLE) {
                    removeChannel(ctx.channel(), "idle");
                    ctx.close();
                    return;
                }
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("NettyClientHandler exception", cause);
            removeChannel(ctx.channel(), "exception");
            ctx.close();
        }
    }

    private List<String> parseTargetAddrs(String addr) {
        if (addr == null) {
            return Collections.emptyList();
        }

        String trimmed = addr.trim();
        if (trimmed.isEmpty()) {
            return Collections.emptyList();
        }
        if (trimmed.indexOf(';') < 0) {
            return Collections.singletonList(trimmed);
        }

        String[] parts = trimmed.split(";");
        List<String> addrs = new ArrayList<>(parts.length);
        for (String part : parts) {
            String candidate = part.trim();
            if (!candidate.isEmpty()) {
                addrs.add(candidate);
            }
        }
        return addrs;
    }
}
