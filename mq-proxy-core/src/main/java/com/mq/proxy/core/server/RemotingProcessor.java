package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import io.netty.channel.Channel;

public interface RemotingProcessor {
    RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception;
}
