package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import io.netty.channel.Channel;

import java.util.concurrent.CountDownLatch;

public class ResponseFuture {
    private final int opaque;
    private final Channel processChannel;
    private final long timeoutMillis;
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private final long beginTimestamp = System.currentTimeMillis();
    private volatile RemotingCommand responseCommand;
    private volatile boolean sendRequestOK = true;
    private volatile Throwable cause;

    public ResponseFuture(int opaque, Channel processChannel, long timeoutMillis) {
        this.opaque = opaque;
        this.processChannel = processChannel;
        this.timeoutMillis = timeoutMillis;
    }

    public RemotingCommand waitResponse(long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    public void putResponse(RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
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
}
