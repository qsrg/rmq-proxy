package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;

public interface InvokeCallback {
    void operationSucceed(RemotingCommand response);

    void operationFail(Throwable throwable);
}
