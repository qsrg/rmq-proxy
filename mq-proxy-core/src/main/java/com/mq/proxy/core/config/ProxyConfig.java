package com.mq.proxy.core.config;

public class ProxyConfig {
    private int listenPort = 19876;
    private String proxyHost;
    private String namesrvAddr = "127.0.0.1:9876";
    private int connectTimeoutMillis = 3000;
    private int bossThreadNums = 1;
    private int workerThreadNums = Runtime.getRuntime().availableProcessors();
    private int pullExecutorThreadNums = Math.max(32, Runtime.getRuntime().availableProcessors() * 4);
    private long routeCacheExpireMillis = 30000;

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
