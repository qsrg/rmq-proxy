package com.mq.proxy.sdk.exception;

public class ProxyTimeoutException extends ProxyException {
    private static final long serialVersionUID = 1L;
    
    private long timeoutMillis;
    
    public ProxyTimeoutException(String message, long timeoutMillis) {
        super(message + " (timeout: " + timeoutMillis + "ms)");
        this.timeoutMillis = timeoutMillis;
    }
    
    public long getTimeoutMillis() {
        return timeoutMillis;
    }
}
