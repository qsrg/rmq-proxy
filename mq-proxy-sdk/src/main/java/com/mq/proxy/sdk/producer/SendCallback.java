package com.mq.proxy.sdk.producer;

public interface SendCallback {

    void onSuccess(SendResult sendResult);

    void onException(Throwable cause);
}