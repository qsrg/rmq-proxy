package com.mq.proxy.sdk.remoting;

import com.mq.proxy.core.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;

public interface SDKRequestProcessor {
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;
}
