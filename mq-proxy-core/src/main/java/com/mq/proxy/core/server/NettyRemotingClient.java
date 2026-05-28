package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.codec.RemotingCommandDecoder;
import com.mq.proxy.core.protocol.codec.RemotingCommandEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRemotingClient {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingClient.class);

    private final NettyClientConfig nettyClientConfig;
    private Bootstrap bootstrap;
    private EventLoopGroup eventLoopGroup;
    private SslContext sslContext;
    private final ConcurrentHashMap<String, Channel> channelTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>();
    private final AtomicInteger opaqueCounter = new AtomicInteger(0);
    private ExecutorService callbackExecutor;
    private final ConcurrentHashMap<Integer, RemotingProcessor> processorTable = new ConcurrentHashMap<>();

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

        if (nettyClientConfig.getTlsKeyStorePath() != null) {
            KeyStore keyStore = KeyStore.getInstance(nettyClientConfig.getTlsKeyStoreType());
            try (InputStream kis = new FileInputStream(nettyClientConfig.getTlsKeyStorePath())) {
                keyStore.load(kis, nettyClientConfig.getTlsKeyStorePassword() != null
                        ? nettyClientConfig.getTlsKeyStorePassword().toCharArray() : null);
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, nettyClientConfig.getTlsKeyStorePassword() != null
                    ? nettyClientConfig.getTlsKeyStorePassword().toCharArray() : null);
            builder.keyManager(kmf);
        }

        if (nettyClientConfig.getTlsTrustStorePath() != null) {
            KeyStore trustStore = KeyStore.getInstance(nettyClientConfig.getTlsKeyStoreType());
            try (InputStream tis = new FileInputStream(nettyClientConfig.getTlsTrustStorePath())) {
                trustStore.load(tis, nettyClientConfig.getTlsTrustStorePassword() != null
                        ? nettyClientConfig.getTlsTrustStorePassword().toCharArray() : null);
            }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            builder.trustManager(tmf);
        } else {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            log.warn("TLS enabled without trustStore, using InsecureTrustManagerFactory (not for production)");
        }

        return builder.build();
    }

    public void start() {
        this.eventLoopGroup = new NioEventLoopGroup(this.nettyClientConfig.getClientWorkerThreadNums());
        this.callbackExecutor = Executors.newFixedThreadPool(4);

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
                        ch.pipeline().addLast(new RemotingCommandDecoder());
                        ch.pipeline().addLast(new RemotingCommandEncoder());
                        ch.pipeline().addLast(new NettyClientHandler());
                    }
                });
    }

    public void shutdown() {
        for (Channel channel : this.channelTable.values()) {
            channel.close();
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
        String[] parts = addr.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        ChannelFuture channelFuture = this.bootstrap.connect(host, port).sync();
        channel = channelFuture.channel();
        this.channelTable.put(addr, channel);
        return channel;
    }

    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        Channel channel = getAndCreateChannel(addr);
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("channel is not active, addr: " + addr);
        }
        request.setOpaque(this.opaqueCounter.getAndIncrement());
        ResponseFuture responseFuture = new ResponseFuture(request.getOpaque(), channel, timeoutMillis);
        this.responseTable.put(request.getOpaque(), responseFuture);
        try {
            channel.writeAndFlush(request);
            RemotingCommand response = responseFuture.waitResponse(timeoutMillis);
            if (response == null) {
                throw new RuntimeException("invokeSync timeout, addr: " + addr + ", timeoutMillis: " + timeoutMillis);
            }
            return response;
        } finally {
            this.responseTable.remove(request.getOpaque());
        }
    }

    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        Channel channel = getAndCreateChannel(addr);
        if (channel == null || !channel.isActive()) {
            throw new RuntimeException("channel is not active, addr: " + addr);
        }
        request.markOnewayRPC();
        channel.writeAndFlush(request);
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
                    RemotingCommand response = processor.processRequest(ctx.channel(), msg);
                    if (!msg.isOnewayRPC() && response != null) {
                        response.setOpaque(msg.getOpaque());
                        ctx.writeAndFlush(response);
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("NettyClientHandler exception", cause);
            ctx.close();
        }
    }
}
