package com.mq.proxy.core.protocol;

public class ResponseCode {
    public static final int FLUSH_DISK_TIMEOUT = 10;
    public static final int SLAVE_NOT_AVAILABLE = 11;
    public static final int FLUSH_SLAVE_TIMEOUT = 12;
    public static final int MESSAGE_ILLEGAL = 13;
    public static final int SERVICE_NOT_AVAILABLE = 14;
    public static final int NO_PERMISSION = 16;
    public static final int TOPIC_NOT_EXIST = 17;
    public static final int PULL_NOT_FOUND = 19;
    public static final int PULL_RETRY_IMMEDIATELY = 20;
    public static final int PULL_OFFSET_MOVED = 21;
    public static final int FLOW_CONTROL = 215;
}
