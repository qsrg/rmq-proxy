package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResponseFuture {
    private final int opaque;
    private final Channel processChannel;
    private final long timeoutMillis;
    private final String remoteAddr;
    private final InvokeCallback invokeCallback;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final long beginTimestamp = System.currentTimeMillis();
    private final AtomicBoolean completed = new AtomicBoolean(false);
    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    public ResponseFuture(int opaque, Channel processChannel, long timeoutMillis, String remoteAddr) {
        this(opaque, processChannel, timeoutMillis, remoteAddr, null);
    }

    public ResponseFuture(int opaque, Channel processChannel, long timeoutMillis, String remoteAddr,
                          InvokeCallback invokeCallback) {
        this.opaque = opaque;
        this.processChannel = processChannel;
        this.timeoutMillis = timeoutMillis;
        this.remoteAddr = remoteAddr;
        this.invokeCallback = invokeCallback;
    }

    public RemotingCommand waitResponse(long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public boolean putResponse(RemotingCommand responseCommand) {
        if (!this.completed.compareAndSet(false, true)) {
            return false;
        }
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
        return true;
    }

    public boolean fail(Throwable cause) {
        if (!this.completed.compareAndSet(false, true)) {
            return false;
        }
        this.cause = cause;
        this.countDownLatch.countDown();
        return true;
    }

    public int getOpaque() {
        return opaque;
    }

    public Channel getProcessChannel() {
        return processChannel;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public RemotingCommand getResponseCommand() {
        return responseCommand;
    }

    public void setResponseCommand(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
    }

    public boolean isSendRequestOK() {
        return sendRequestOK;
    }

    public void setSendRequestOK(boolean sendRequestOK) {
        this.sendRequestOK = sendRequestOK;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public InvokeCallback getInvokeCallback() {
        return invokeCallback;
    }

    public boolean isTimeout() {
        return System.currentTimeMillis() - beginTimestamp > timeoutMillis;
    }

    public boolean isAsync() {
        return invokeCallback != null;
    }
}
