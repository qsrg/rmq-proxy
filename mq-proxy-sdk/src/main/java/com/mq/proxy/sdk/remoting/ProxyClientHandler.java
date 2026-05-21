package com.mq.proxy.sdk.remoting;

import com.mq.proxy.core.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static final Logger log = LoggerFactory.getLogger(ProxyClientHandler.class);

    private final ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable;
    private final ConcurrentHashMap<Integer, SDKRequestProcessor> processorTable;

    public ProxyClientHandler(ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable,
                              ConcurrentHashMap<Integer, SDKRequestProcessor> processorTable) {
        this.responseTable = responseTable;
        this.processorTable = processorTable;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        if (msg.isResponseType()) {
            handleResponse(msg);
        } else {
            handleRequest(ctx, msg);
        }
    }

    private void handleResponse(RemotingCommand msg) {
        int opaque = msg.getOpaque();
        ProxyResponseFuture future = responseTable.remove(opaque);
        if (future != null) {
            future.complete(msg);
        } else {
            log.warn("Receive response, but not found the request, opaque={}", opaque);
        }
    }

    private void handleRequest(ChannelHandlerContext ctx, RemotingCommand msg) {
        int requestCode = msg.getCode();
        SDKRequestProcessor processor = processorTable.get(requestCode);
        if (processor != null) {
            try {
                processor.processRequest(ctx, msg);
            } catch (Exception e) {
                log.error("Process server-push request error, code={}, opaque={}", requestCode, msg.getOpaque(), e);
            }
        } else {
            log.warn("Receive server-push request, but no processor registered, code={}, opaque={}", requestCode, msg.getOpaque());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Channel exception: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}