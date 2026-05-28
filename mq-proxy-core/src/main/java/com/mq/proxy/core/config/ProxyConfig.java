package com.mq.proxy.core.config;

public class ProxyConfig {
    private int listenPort = 10911;
    private String proxyHost = "127.0.0.1";
    private String namesrvAddr = "127.0.0.1:9876";
    private String brokerAddr;
    private int connectTimeoutMillis = 3000;
    private boolean registerProxyToNameServer = false;
    private String proxyBrokerName = "ProxyBroker";
    private String proxyClusterName = "ProxyCluster";
    private int bossThreadNums = 1;
    private int workerThreadNums = Runtime.getRuntime().availableProcessors();
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

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public boolean isRegisterProxyToNameServer() {
        return registerProxyToNameServer;
    }

    public void setRegisterProxyToNameServer(boolean registerProxyToNameServer) {
        this.registerProxyToNameServer = registerProxyToNameServer;
    }

    public String getProxyBrokerName() {
        return proxyBrokerName;
    }

    public void setProxyBrokerName(String proxyBrokerName) {
        this.proxyBrokerName = proxyBrokerName;
    }

    public String getProxyClusterName() {
        return proxyClusterName;
    }

    public void setProxyClusterName(String proxyClusterName) {
        this.proxyClusterName = proxyClusterName;
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
