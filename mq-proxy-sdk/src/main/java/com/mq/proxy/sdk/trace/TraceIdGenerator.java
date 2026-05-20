package com.mq.proxy.sdk.trace;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicLong;

public class TraceIdGenerator {
    
    private static final AtomicLong sequence = new AtomicLong(0);
    
    private static String localIP = "127.0.0.1";
    private static String processId = "0";
    
    static {
        try {
            localIP = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
        }
        
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            processId = name.split("@")[0];
        } catch (Exception e) {
        }
    }
    
    public static String generate() {
        long timestamp = System.currentTimeMillis();
        long seq = sequence.incrementAndGet() % 10000;
        
        return String.format("%s@%s@%d@%04d", localIP, processId, timestamp, seq);
    }
}
