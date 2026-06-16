package com.mq.proxy.core.server;

public class NettyServerConfig {
    private int listenPort = 19876;
    private int bossThreadNums = 1;
    private int workerThreadNums = Runtime.getRuntime().availableProcessors();
    private int requestProcessorThreadNums = Runtime.getRuntime().availableProcessors() * 2;
    private int pullExecutorThreadNums = Math.max(32, Runtime.getRuntime().availableProcessors() * 4);
    private int callbackExecutorThreadNums = 4;
    private int serverOnewaySemaphoreValue = 256;
    private int serverAsyncSemaphoreValue = 64;
    private int serverChannelMaxIdleTimeSeconds = 120;
    private int serverSocketSndBufSize = 65535;
    private int serverSocketRcvBufSize = 65535;

    private boolean tlsEnabled = false;
    private String tlsCertPath;
    private String tlsKeyPath;
    private String tlsTrustCertPath;
    private boolean tlsClientAuth = false;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public int getBossThreadNums() {
        return bossThreadNums;
    }

    public void setBossThreadNums(int bossThreadNums) {
        this.bossThreadNums = bossThreadNums;
    }

    public int getWorkerThreadNums() {
        return workerThreadNums;
    }

    public void setWorkerThreadNums(int workerThreadNums) {
        this.workerThreadNums = workerThreadNums;
    }

    public int getRequestProcessorThreadNums() {
        return requestProcessorThreadNums;
    }

    public void setRequestProcessorThreadNums(int requestProcessorThreadNums) {
        this.requestProcessorThreadNums = requestProcessorThreadNums;
    }

    public int getCallbackExecutorThreadNums() {
        return callbackExecutorThreadNums;
    }

    public void setCallbackExecutorThreadNums(int callbackExecutorThreadNums) {
        this.callbackExecutorThreadNums = callbackExecutorThreadNums;
    }

    public int getPullExecutorThreadNums() {
        return pullExecutorThreadNums;
    }

    public void setPullExecutorThreadNums(int pullExecutorThreadNums) {
        this.pullExecutorThreadNums = pullExecutorThreadNums;
    }

    public int getServerOnewaySemaphoreValue() {
        return serverOnewaySemaphoreValue;
    }

    public void setServerOnewaySemaphoreValue(int serverOnewaySemaphoreValue) {
        this.serverOnewaySemaphoreValue = serverOnewaySemaphoreValue;
    }

    public int getServerAsyncSemaphoreValue() {
        return serverAsyncSemaphoreValue;
    }

    public void setServerAsyncSemaphoreValue(int serverAsyncSemaphoreValue) {
        this.serverAsyncSemaphoreValue = serverAsyncSemaphoreValue;
    }

    public int getServerChannelMaxIdleTimeSeconds() {
        return serverChannelMaxIdleTimeSeconds;
    }

    public void setServerChannelMaxIdleTimeSeconds(int serverChannelMaxIdleTimeSeconds) {
        this.serverChannelMaxIdleTimeSeconds = serverChannelMaxIdleTimeSeconds;
    }

    public int getServerSocketSndBufSize() {
        return serverSocketSndBufSize;
    }

    public void setServerSocketSndBufSize(int serverSocketSndBufSize) {
        this.serverSocketSndBufSize = serverSocketSndBufSize;
    }

    public int getServerSocketRcvBufSize() {
        return serverSocketRcvBufSize;
    }

    public void setServerSocketRcvBufSize(int serverSocketRcvBufSize) {
        this.serverSocketRcvBufSize = serverSocketRcvBufSize;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    public void setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
    }

    public String getTlsCertPath() {
        return tlsCertPath;
    }

    public void setTlsCertPath(String tlsCertPath) {
        this.tlsCertPath = tlsCertPath;
    }

    public String getTlsKeyPath() {
        return tlsKeyPath;
    }

    public void setTlsKeyPath(String tlsKeyPath) {
        this.tlsKeyPath = tlsKeyPath;
    }

    public String getTlsTrustCertPath() {
        return tlsTrustCertPath;
    }

    public void setTlsTrustCertPath(String tlsTrustCertPath) {
        this.tlsTrustCertPath = tlsTrustCertPath;
    }

    public boolean isTlsClientAuth() {
        return tlsClientAuth;
    }

    public void setTlsClientAuth(boolean tlsClientAuth) {
        this.tlsClientAuth = tlsClientAuth;
    }
}
