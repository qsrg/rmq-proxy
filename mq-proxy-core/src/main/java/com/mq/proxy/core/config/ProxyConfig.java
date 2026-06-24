package com.mq.proxy.core.config;

import com.mq.proxy.core.server.TlsMode;

public class ProxyConfig {
    private int listenPort = 19876;
    private String proxyHost;
    private String namesrvAddr = "127.0.0.1:9876";
    private int connectTimeoutMillis = 3000;
    private int bossThreadNums = 1;
    private int workerThreadNums = Runtime.getRuntime().availableProcessors();
    private int requestProcessorThreadNums = Runtime.getRuntime().availableProcessors() * 2;
    private int pullExecutorThreadNums = Math.max(32, Runtime.getRuntime().availableProcessors() * 4);
    private long routeCacheExpireMillis = 30000;
    private int upstreamClientAsyncSemaphoreValue = 4096;
    private int upstreamProducerAsyncSemaphoreValue = 4096;
    private int upstreamPullAsyncSemaphoreValue = 4096;
    private int upstreamClientChannelPoolSize = 4;
    private int upstreamClientKeepAliveIntervalSeconds = 30;

    private boolean tlsEnabled = false;
    private String tlsCertPath;
    private String tlsKeyPath;
    private String tlsTrustCertPath;
    private boolean tlsClientAuth = false;
    /**
     * TLS 模式：disabled / permissive / enforcing。
     * 当 tlsEnabled=true 时默认为 permissive（动态 TLS 检测，同时支持 TLS 和非 TLS 客户端）。
     */
    private TlsMode tlsMode = TlsMode.PERMISSIVE;

    /**
     * 上游（proxy -> broker）TLS 配置，与下游 TLS 独立。
     * 语义与 RocketMQ 客户端 useTLS(true) 一致：开启 TLS 加密传输，
     * 默认不验证 broker 证书（与 RocketMQ 默认 tlsClientAuthServer=false 行为一致）。
     * 当 broker 配置为 tlsMode=enforcing 时必须启用；
     * 当 broker 为 permissive 时可选启用以实现端到端加密。
     */
    private boolean upstreamTlsEnabled = false;
    private String upstreamTlsClientCertPath;
    private String upstreamTlsClientKeyPath;

    public int getListenPort() {
        return listenPort;
    }

    public void setListenPort(int listenPort) {
        this.listenPort = listenPort;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
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

    public int getPullExecutorThreadNums() {
        return pullExecutorThreadNums;
    }

    public void setPullExecutorThreadNums(int pullExecutorThreadNums) {
        this.pullExecutorThreadNums = pullExecutorThreadNums;
    }

    public long getRouteCacheExpireMillis() {
        return routeCacheExpireMillis;
    }

    public void setRouteCacheExpireMillis(long routeCacheExpireMillis) {
        this.routeCacheExpireMillis = routeCacheExpireMillis;
    }

    public int getUpstreamClientAsyncSemaphoreValue() {
        return upstreamClientAsyncSemaphoreValue;
    }

    public void setUpstreamClientAsyncSemaphoreValue(int upstreamClientAsyncSemaphoreValue) {
        this.upstreamClientAsyncSemaphoreValue = upstreamClientAsyncSemaphoreValue;
        this.upstreamProducerAsyncSemaphoreValue = upstreamClientAsyncSemaphoreValue;
        this.upstreamPullAsyncSemaphoreValue = upstreamClientAsyncSemaphoreValue;
    }

    public int getUpstreamProducerAsyncSemaphoreValue() {
        return upstreamProducerAsyncSemaphoreValue;
    }

    public void setUpstreamProducerAsyncSemaphoreValue(int upstreamProducerAsyncSemaphoreValue) {
        this.upstreamProducerAsyncSemaphoreValue = upstreamProducerAsyncSemaphoreValue;
    }

    public int getUpstreamPullAsyncSemaphoreValue() {
        return upstreamPullAsyncSemaphoreValue;
    }

    public void setUpstreamPullAsyncSemaphoreValue(int upstreamPullAsyncSemaphoreValue) {
        this.upstreamPullAsyncSemaphoreValue = upstreamPullAsyncSemaphoreValue;
    }

    public int getUpstreamClientChannelPoolSize() {
        return upstreamClientChannelPoolSize;
    }

    public void setUpstreamClientChannelPoolSize(int upstreamClientChannelPoolSize) {
        this.upstreamClientChannelPoolSize = upstreamClientChannelPoolSize;
    }

    public int getUpstreamClientKeepAliveIntervalSeconds() {
        return upstreamClientKeepAliveIntervalSeconds;
    }

    public void setUpstreamClientKeepAliveIntervalSeconds(int upstreamClientKeepAliveIntervalSeconds) {
        this.upstreamClientKeepAliveIntervalSeconds = upstreamClientKeepAliveIntervalSeconds;
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

    public TlsMode getTlsMode() {
        return tlsMode;
    }

    public void setTlsMode(TlsMode tlsMode) {
        this.tlsMode = tlsMode;
    }

    public boolean isUpstreamTlsEnabled() {
        return upstreamTlsEnabled;
    }

    public void setUpstreamTlsEnabled(boolean upstreamTlsEnabled) {
        this.upstreamTlsEnabled = upstreamTlsEnabled;
    }

    public String getUpstreamTlsClientCertPath() {
        return upstreamTlsClientCertPath;
    }

    public void setUpstreamTlsClientCertPath(String upstreamTlsClientCertPath) {
        this.upstreamTlsClientCertPath = upstreamTlsClientCertPath;
    }

    public String getUpstreamTlsClientKeyPath() {
        return upstreamTlsClientKeyPath;
    }

    public void setUpstreamTlsClientKeyPath(String upstreamTlsClientKeyPath) {
        this.upstreamTlsClientKeyPath = upstreamTlsClientKeyPath;
    }
}
