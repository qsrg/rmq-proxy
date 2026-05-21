package com.mq.proxy.sdk.remoting;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.codec.RemotingCommandEncoder;
import com.mq.proxy.core.protocol.codec.RemotingCommandDecoder;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyRemotingClient {

    private static final Logger log = LoggerFactory.getLogger(ProxyRemotingClient.class);

    private final ProxyClientConfig config;
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;

    private final AtomicInteger opaqueGenerator = new AtomicInteger(0);

    private final ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable =
        new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, SDKRequestProcessor> processorTable =
        new ConcurrentHashMap<>();

    public ProxyRemotingClient(ProxyClientConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.getWorkerThreadNums());
        this.bootstrap = createBootstrap();
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

    public void shutdown() {
        eventLoopGroup.shutdownGracefully();
    }
}