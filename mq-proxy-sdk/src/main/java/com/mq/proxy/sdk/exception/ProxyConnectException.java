package com.mq.proxy.sdk.exception;

public class ProxyConnectException extends ProxyException {
    private static final long serialVersionUID = 1L;
    
    private String proxyAddr;
    
    public ProxyConnectException(String proxyAddr) {
        super("Failed to connect to proxy: " + proxyAddr);
        this.proxyAddr = proxyAddr;
    }
    
    public ProxyConnectException(String proxyAddr, String message) {
        super("Failed to connect to proxy [" + proxyAddr + "]: " + message);
        this.proxyAddr = proxyAddr;
    }
    
    public ProxyConnectException(String proxyAddr, Throwable cause) {
        super("Failed to connect to proxy: " + proxyAddr, cause);
        this.proxyAddr = proxyAddr;
    }
    
    public String getProxyAddr() {
        return proxyAddr;
    }
}
