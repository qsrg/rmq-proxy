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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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

    public ProxyRemotingClient(ProxyCommonConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getWorkerThreadNums());
        this.asyncCallbackExecutor = java.util.concurrent.Executors.newFixedThreadPool(4);
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

        if (config.getTlsKeyStorePath() != null) {
            KeyStore keyStore = KeyStore.getInstance(config.getTlsKeyStoreType());
            try (InputStream kis = new FileInputStream(config.getTlsKeyStorePath())) {
                keyStore.load(kis, config.getTlsKeyStorePassword() != null
                        ? config.getTlsKeyStorePassword().toCharArray() : null);
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, config.getTlsKeyStorePassword() != null
                    ? config.getTlsKeyStorePassword().toCharArray() : null);
            builder.keyManager(kmf);
        }

        if (config.getTlsTrustStorePath() != null) {
            KeyStore trustStore = KeyStore.getInstance(config.getTlsKeyStoreType());
            try (InputStream tis = new FileInputStream(config.getTlsTrustStorePath())) {
                trustStore.load(tis, config.getTlsTrustStorePassword() != null
                        ? config.getTlsTrustStorePassword().toCharArray() : null);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            builder.trustManager(tmf);
        } else {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            log.warn("SDK TLS enabled without trustStore, using InsecureTrustManagerFactory (not for production)");
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
                   .addLast(new ProxyClientHandler(responseTable, processorTable));
             }
         });

        return b;
    }

    public Bootstrap getBootstrap() {
        return bootstrap;
    }

    public void start() {
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
    }

    public void invokeOneway(Channel channel, RemotingCommand request) {
        request.markOnewayRPC();
        int opaque = opaqueGenerator.incrementAndGet();
        request.setOpaque(opaque);
        channel.writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                log.warn("invokeOneway failed, opaque={}, error={}", opaque, f.cause().getMessage());
            }
        });
    }

    public void shutdown() {
        asyncCallbackExecutor.shutdown();
        eventLoopGroup.shutdownGracefully();
    }
}