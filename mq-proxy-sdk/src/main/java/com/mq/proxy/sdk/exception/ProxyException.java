package com.mq.proxy.sdk.exception;

public class ProxyException extends Exception {
    private static final long serialVersionUID = 1L;
    
    public ProxyException(String message) {
        super(message);
    }
    
    public ProxyException(String message, Throwable cause) {
        super(message, cause);
    }
}
