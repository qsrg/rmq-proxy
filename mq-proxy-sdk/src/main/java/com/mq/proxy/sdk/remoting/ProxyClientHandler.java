package com.mq.proxy.sdk.remoting;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class ProxyClientHandler extends SimpleChannelInboundHandler<Object> {
    
    private static final Logger log = LoggerFactory.getLogger(ProxyClientHandler.class);
    
    private final ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable;
    
    public ProxyClientHandler(ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable) {
        this.responseTable = responseTable;
    }
    
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof io.netty.buffer.ByteBuf) {
            io.netty.buffer.ByteBuf buf = (io.netty.buffer.ByteBuf) msg;
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            buf.release();
            
            processResponse(data);
        }
    }
    
    private void processResponse(byte[] data) {
        try {
            int opaque = parseOpaque(data);
            
            ProxyResponseFuture future = responseTable.remove(opaque);
            if (future != null) {
                future.complete(data);
            } else {
                log.warn("Receive response, but not found the request, opaque={}", opaque);
            }
        } catch (Exception e) {
            log.error("Process response error", e);
        }
    }
    
    private int parseOpaque(byte[] data) {
        if (data.length < 8) {
            return 0;
        }
        
        int opaque = ((data[4] & 0xFF) << 24) |
                    ((data[5] & 0xFF) << 16) |
                    ((data[6] & 0xFF) << 8) |
                    (data[7] & 0xFF);
        
        return opaque;
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Channel exception: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
