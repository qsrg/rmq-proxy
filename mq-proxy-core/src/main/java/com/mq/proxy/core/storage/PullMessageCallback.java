package com.mq.proxy.core.storage;

import com.mq.proxy.core.storage.model.PullResult;

public interface PullMessageCallback {
    void onSuccess(PullResult pullResult);

    void onException(Throwable throwable);
}
