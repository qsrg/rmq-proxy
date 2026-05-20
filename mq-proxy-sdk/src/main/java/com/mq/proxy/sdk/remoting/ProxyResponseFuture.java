package com.mq.proxy.sdk.remoting;

import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ProxyResponseFuture {
    
    private final int opaque;
    private final Channel channel;
    private final long timeoutMillis;
    private final CountDownLatch latch = new CountDownLatch(1);
    
    private volatile Object response;
    private volatile Throwable cause;
    
    public ProxyResponseFuture(int opaque, Channel channel, long timeoutMillis) {
        this.opaque = opaque;
        this.channel = channel;
        this.timeoutMillis = timeoutMillis;
    }
    
    public void complete(Object response) {
        this.response = response;
        latch.countDown();
    }
    
    public void completeExceptionally(Throwable cause) {
        this.cause = cause;
        latch.countDown();
    }
    
    public Object waitResponse() throws InterruptedException {
        latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return response;
    }
    
    public boolean isTimeout() {
        return latch.getCount() > 0;
    }
    
    public int getOpaque() {
        return opaque;
    }
    
    public Channel getChannel() {
        return channel;
    }
    
    public Object getResponse() {
        return response;
    }
    
    public Throwable getCause() {
        return cause;
    }
}
