package com.mq.proxy.core.server;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.codec.RemotingCommandDecoder;
import com.mq.proxy.core.protocol.codec.RemotingCommandEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;
import java.io.FileInputStream;
import java.io.InputStream;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NettyRemotingServer {

    private static final Logger log = LoggerFactory.getLogger(NettyRemotingServer.class);

    private final NettyServerConfig nettyServerConfig;
    private ServerBootstrap serverBootstrap;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    private final ConcurrentHashMap<Integer, RemotingProcessor> processorTable = new ConcurrentHashMap<>();
    private RemotingProcessor defaultRemotingProcessor = new DefaultRemotingProcessor();
    private final AtomicInteger opaqueCounter = new AtomicInteger(0);
    private final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>();
    private ExecutorService callbackExecutor;
    private ScheduledExecutorService channelScanExecutor;

    private ClientConnectionManager clientConnectionManager;
    private SslContext sslContext;
    private ExecutorService defaultExecutor;

    public NettyRemotingServer(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        if (nettyServerConfig.isTlsEnabled()) {
            try {
                this.sslContext = buildSslContext();
                log.info("TLS enabled, SSL context initialized");
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize TLS SSL context", e);
            }
        }
    }

    private SslContext buildSslContext() throws Exception {
        InputStream certStream = null;
        InputStream keyStream = null;
        try {
            certStream = new FileInputStream(nettyServerConfig.getTlsCertPath());
            keyStream = new FileInputStream(nettyServerConfig.getTlsKeyPath());

            String keyPassword = System.getProperty("proxy.tlsKeyPassword");
            SslContextBuilder builder = SslContextBuilder.forServer(
                    certStream, keyStream, keyPassword);

            if (nettyServerConfig.isTlsClientAuth()) {
                if (nettyServerConfig.getTlsTrustCertPath() != null) {
                    InputStream trustCertStream = new FileInputStream(nettyServerConfig.getTlsTrustCertPath());
                    builder.trustManager(trustCertStream);
                }
                builder.clientAuth(ClientAuth.REQUIRE);
            }

            return builder.build();
        } finally {
            if (certStream != null) {
                certStream.close();
            }
            if (keyStream != null) {
                keyStream.close();
            }
        }
    }

    public void setClientConnectionManager(ClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    public void start() {
        this.bossGroup = new NioEventLoopGroup(this.nettyServerConfig.getBossThreadNums());
        this.workerGroup = new NioEventLoopGroup(this.nettyServerConfig.getWorkerThreadNums());
        this.callbackExecutor = Executors.newFixedThreadPool(this.nettyServerConfig.getCallbackExecutorThreadNums());
        this.defaultExecutor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2,
                r -> {
                    Thread t = new Thread(r, "ProxyRequestProcessor");
                    t.setDaemon(true);
                    return t;
                });
        this.channelScanExecutor = Executors.newSingleThreadScheduledExecutor();

        this.serverBootstrap = new ServerBootstrap();
        this.serverBootstrap.group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_SNDBUF, this.nettyServerConfig.getServerSocketSndBufSize())
                .option(ChannelOption.SO_RCVBUF, this.nettyServerConfig.getServerSocketRcvBufSize())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        if (sslContext != null) {
                            SSLEngine sslEngine = sslContext.newEngine(ch.alloc());
                            ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
                        }
                        ch.pipeline().addLast("frameDecoder",
                                new LengthFieldBasedFrameDecoder(16777216, 0, 4, 0, 0));
                        ch.pipeline().addLast("idleStateHandler",
                                new IdleStateHandler(nettyServerConfig.getServerChannelMaxIdleTimeSeconds(),
                                        nettyServerConfig.getServerChannelMaxIdleTimeSeconds(),
                                        nettyServerConfig.getServerChannelMaxIdleTimeSeconds()));
                        ch.pipeline().addLast("decoder", new RemotingCommandDecoder());
                        ch.pipeline().addLast("encoder", new RemotingCommandEncoder());
                        ch.pipeline().addLast("handler", new NettyServerHandler());
                    }
                });

        try {
            ChannelFuture channelFuture = this.serverBootstrap.bind(this.nettyServerConfig.getListenPort()).sync();
            this.serverChannel = channelFuture.channel();
            log.info("NettyRemotingServer started on port {}", this.nettyServerConfig.getListenPort());
        } catch (InterruptedException e) {
            throw new RuntimeException("NettyRemotingServer start failed", e);
        }

        this.channelScanExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (clientConnectionManager != null) {
                        clientConnectionManager.scanNotActiveChannel();
                    }
                } catch (Throwable e) {
                    log.error("scanNotActiveChannel exception", e);
                }
            }
        }, 10, 10, TimeUnit.SECONDS);
        log.info("Channel scan scheduler started, interval=10s");
    }

    public void shutdown() {
        if (this.channelScanExecutor != null) {
            this.channelScanExecutor.shutdown();
        }
        if (this.serverChannel != null) {
            this.serverChannel.close();
        }
        if (this.bossGroup != null) {
            this.bossGroup.shutdownGracefully();
        }
        if (this.workerGroup != null) {
            this.workerGroup.shutdownGracefully();
        }
        if (this.callbackExecutor != null) {
            this.callbackExecutor.shutdown();
        }
        if (this.defaultExecutor != null) {
            this.defaultExecutor.shutdown();
        }
        for (ResponseFuture responseFuture : this.responseTable.values()) {
            responseFuture.putResponse(null);
        }
        this.responseTable.clear();
    }

    public void registerProcessor(int requestCode, RemotingProcessor processor) {
        this.processorTable.put(requestCode, processor);
    }

    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("Channel ACTIVE: {}", ctx.channel().remoteAddress());
            super.channelActive(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.info("Channel INACTIVE: {}", ctx.channel().remoteAddress());
            if (clientConnectionManager != null) {
                clientConnectionManager.onChannelInactive(ctx.channel());
            }
            super.channelInactive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state() == IdleState.ALL_IDLE) {
                    log.warn("Channel idle timeout, closing: {}", ctx.channel().remoteAddress());
                    if (clientConnectionManager != null) {
                        clientConnectionManager.onChannelInactive(ctx.channel());
                    }
                    ctx.close();
                }
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }

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
                if (processor == null) {
                    processor = defaultRemotingProcessor;
                }
                final RemotingProcessor finalProcessor = processor;
                defaultExecutor.submit(() -> {
                    try {
                        RemotingCommand response = finalProcessor.processRequest(ctx.channel(), msg);
                        if (!msg.isOnewayRPC() && response != null) {
                            response.setOpaque(msg.getOpaque());
                            response.setSerializeTypeCurrentRPC(msg.getSerializeTypeCurrentRPC());
                            ctx.writeAndFlush(response);
                        }
                    } catch (Exception e) {
                        log.error("processRequest exception, code={}, remoteAddr={}",
                                msg.getCode(), ctx.channel().remoteAddress(), e);
                        if (!msg.isOnewayRPC()) {
                            RemotingCommand errorResponse = RemotingCommand.createResponseCommand(
                                    com.mq.proxy.core.protocol.RemotingSysResponseCode.SYSTEM_ERROR);
                            errorResponse.setOpaque(msg.getOpaque());
                            errorResponse.setRemark(e.getMessage());
                            ctx.writeAndFlush(errorResponse);
                        }
                    }
                });
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.error("NettyServerHandler exception, remoteAddr={}", ctx.channel().remoteAddress(), cause);
            if (clientConnectionManager != null) {
                clientConnectionManager.onChannelInactive(ctx.channel());
            }
            ctx.close();
        }
    }
}
