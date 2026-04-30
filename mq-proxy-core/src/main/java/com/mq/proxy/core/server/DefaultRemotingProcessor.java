package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import io.netty.channel.Channel;

public class DefaultRemotingProcessor implements RemotingProcessor {

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "request code not supported");
    }
}
