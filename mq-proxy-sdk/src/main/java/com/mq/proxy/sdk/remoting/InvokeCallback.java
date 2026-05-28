package com.mq.proxy.sdk.remoting;

import com.mq.proxy.core.protocol.RemotingCommand;

public interface InvokeCallback {

    void onSuccess(RemotingCommand response);

    void onException(Throwable cause);
}
