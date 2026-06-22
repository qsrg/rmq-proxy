package com.mq.proxy.core.storage;

import com.mq.proxy.core.storage.model.PutResult;

public interface PutMessageCallback {
    void onSuccess(PutResult putResult);

    void onException(Throwable throwable);
}
