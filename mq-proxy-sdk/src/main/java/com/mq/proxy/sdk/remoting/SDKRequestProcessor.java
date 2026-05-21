package com.mq.proxy.sdk.remoting;

import io.netty.channel.ChannelHandlerContext;

public interface SDKRequestProcessor {
    void processRequest(ChannelHandlerContext ctx, com.mq.proxy.core.protocol.RemotingCommand request) throws Exception;
}