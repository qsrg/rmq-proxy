package com.mq.proxy.sdk.remoting;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.codec.RemotingCommandEncoder;
import com.mq.proxy.core.protocol.codec.RemotingCommandDecoder;
import com.mq.proxy.sdk.config.ProxyCommonConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyRemotingClient {

    private static final Logger log = LoggerFactory.getLogger(ProxyRemotingClient.class);

    private final ProxyCommonConfig config;
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final SslContext sslContext;

    private final AtomicInteger opaqueGenerator = new AtomicInteger(0);

    private final ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable =
        new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, SDKRequestProcessor> processorTable =
        new ConcurrentHashMap<>();

    private final ExecutorService asyncCallbackExecutor;

    private java.util.Timer timer;

    public ProxyRemotingClient(ProxyCommonConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getWorkerThreadNums());
        this.asyncCallbackExecutor = new ThreadPoolExecutor(4, 4, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(10000),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "AsyncCallbackThread_" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            });
        if (config.isTlsEnabled()) {
            try {
                this.sslContext = buildSslContext();
                log.info("SDK TLS enabled, SSL context initialized");
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize SDK TLS SSL context", e);
            }
        } else {
            this.sslContext = null;
        }
        this.bootstrap = createBootstrap();
    }

    private SslContext buildSslContext() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();

        if (config.getTlsClientCertPath() != null && config.getTlsClientKeyPath() != null) {
            InputStream certStream = null;
            InputStream keyStream = null;
            try {
                certStream = new FileInputStream(config.getTlsClientCertPath());
                keyStream = new FileInputStream(config.getTlsClientKeyPath());
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

        if (config.getTlsTrustCertPath() != null) {
            InputStream trustCertStream = null;
            try {
                trustCertStream = new FileInputStream(config.getTlsTrustCertPath());
                builder.trustManager(trustCertStream);
            } finally {
                if (trustCertStream != null) {
                    trustCertStream.close();
                }
            }
        } else {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            log.warn("SDK TLS enabled without trustCertPath, using InsecureTrustManagerFactory (not for production)");
        }

        return builder.build();
    }

    private Bootstrap createBootstrap() {
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMillis())
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) throws Exception {
                 if (sslContext != null) {
                     SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
                     ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
                 }
                 ch.pipeline()
                   .addLast(new RemotingCommandEncoder())
                   .addLast(new RemotingCommandDecoder())
                   .addLast(new ProxyClientHandler(ProxyRemotingClient.this, responseTable, processorTable));
             }
         });

        return b;
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public void start() {
        this.timer = new java.util.Timer("SDKResponseScan", true);
        this.timer.scheduleAtFixedRate(new java.util.TimerTask() {
            @Override
            public void run() {
                try {
                    scanResponseTable();
                } catch (Throwable e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 3000, 1000);
        log.info("ProxyRemotingClient initialized");
    }

    public void registerProcessor(int requestCode, SDKRequestProcessor processor) {
        processorTable.put(requestCode, processor);
        log.info("Registered SDK processor for request code {}", requestCode);
    }

    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis)
            throws Exception {

        int opaque = opaqueGenerator.incrementAndGet();
        request.setOpaque(opaque);

        ProxyResponseFuture future = new ProxyResponseFuture(opaque, channel, timeoutMillis);
        responseTable.put(opaque, future);

        try {
            channel.writeAndFlush(request).addListener(f -> {
                if (!f.isSuccess()) {
                    responseTable.remove(opaque);
                    future.completeExceptionally(f.cause());
                }
            });

            RemotingCommand response = (RemotingCommand) future.waitResponse();

            if (future.getCause() != null) {
                throw new RuntimeException("Request failed", future.getCause());
            }

            if (response == null) {
                throw new RuntimeException("Request timeout, opaque=" + opaque);
            }

            return response;

        } finally {
            responseTable.remove(opaque);
        }
    }

    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis,
                            InvokeCallback callback) {
        int opaque = opaqueGenerator.incrementAndGet();
        request.setOpaque(opaque);

        ProxyResponseFuture future = new ProxyResponseFuture(opaque, channel, timeoutMillis);
        responseTable.put(opaque, future);

        channel.writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                responseTable.remove(opaque);
                future.completeExceptionally(f.cause());
            }
        });

        try {
            asyncCallbackExecutor.execute(() -> {
                try {
                    RemotingCommand response = (RemotingCommand) future.waitResponse();
                    if (future.getCause() != null) {
                        callback.onException(future.getCause());
                    } else if (response == null) {
                        callback.onException(new RuntimeException("invokeAsync timeout, opaque=" + opaque));
                    } else {
                        callback.onSuccess(response);
                    }
                } catch (InterruptedException e) {
                    callback.onException(e);
                } finally {
                    responseTable.remove(opaque);
                }
            });
        } catch (java.util.concurrent.RejectedExecutionException e) {
            responseTable.remove(opaque);
            future.completeExceptionally(e);
            callback.onException(e);
            log.warn("invokeAsync rejected, opaque={}, executor queue full", opaque);
        }
    }

    public void invokeOneway(Channel channel, RemotingCommand request) {
        request.markOnewayRPC();
        int opaque = opaqueGenerator.incrementAndGet();
        request.setOpaque(opaque);
        channel.writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("invokeOneway failed, opaque={}, error={}", opaque,
                    f.cause() != null ? f.cause().getMessage() : "unknown");
            }
        });
    }

    public void shutdown() {
        if (this.timer != null) {
            this.timer.cancel();
        }
        asyncCallbackExecutor.shutdown();
        eventLoopGroup.shutdownGracefully();
    }

    public void scanResponseTable() {
        Iterator<Map.Entry<Integer, ProxyResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ProxyResponseFuture> next = it.next();
            ProxyResponseFuture future = next.getValue();

            if (future.isTimeout()) {
                it.remove();
                future.completeExceptionally(new RuntimeException("request timeout, opaque=" + future.getOpaque()));
                log.warn("remove timeout request, opaque={}", future.getOpaque());
            }
        }
    }

    public void failFast(Channel channel) {
        Iterator<Map.Entry<Integer, ProxyResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ProxyResponseFuture> entry = it.next();
            if (entry.getValue().getChannel() == channel) {
                it.remove();
                entry.getValue().completeExceptionally(
                    new RuntimeException("channel inactive, opaque=" + entry.getValue().getOpaque()));
                log.warn("failFast: remove pending request for inactive channel, opaque={}", entry.getKey());
            }
        }
    }
}