package com.mq.proxy.sdk.remoting;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
                   .addLast(new ProxyClientHandler(responseTable));
             }
         });
        
        return b;
    }
    
    public Bootstrap getBootstrap() {
        return bootstrap;
    }
    
    public byte[] invokeSync(Channel channel, byte[] request, long timeoutMillis) 
            throws Exception {
        
        int opaque = opaqueGenerator.incrementAndGet();
        
        ProxyResponseFuture future = new ProxyResponseFuture(opaque, channel, timeoutMillis);
        responseTable.put(opaque, future);
        
        try {
            ByteBuf buf = Unpooled.wrappedBuffer(request);
            channel.writeAndFlush(buf).addListener(f -> {
                if (!f.isSuccess()) {
                    responseTable.remove(opaque);
                    future.completeExceptionally(f.cause());
                }
            });
            
            byte[] response = (byte[]) future.waitResponse();
            
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
