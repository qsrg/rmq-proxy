package com.mq.proxy.core.protocol;

public class RequestCode {
    public static final int SEND_MESSAGE = 10;
    public static final int PULL_MESSAGE = 11;
    public static final int QUERY_MESSAGE = 12;
    public static final int QUERY_BROKER_OFFSET = 13;
    public static final int QUERY_CONSUMER_OFFSET = 14;
    public static final int UPDATE_CONSUMER_OFFSET = 15;
    public static final int UPDATE_AND_CREATE_TOPIC = 17;
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    public static final int GET_TOPIC_CONFIG_LIST = 22;
    public static final int GET_TOPIC_NAME_LIST = 23;
    public static final int HEART_BEAT = 34;
    public static final int UNREGISTER_CLIENT = 35;
    public static final int CONSUMER_SEND_MSG_BACK = 36;
    public static final int END_TRANSACTION = 37;
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;
    public static final int LOCK_BATCH_MQ = 41;
    public static final int UNLOCK_BATCH_MQ = 42;
    public static final int SEND_MESSAGE_V2 = 310;
    public static final int SEND_BATCH_MESSAGE = 320;

    public static final int PUT_KV_CONFIG = 100;
    public static final int GET_KV_CONFIG = 101;
    public static final int DELETE_KV_CONFIG = 102;
    public static final int REGISTER_BROKER = 103;
    public static final int UNREGISTER_BROKER = 104;
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;
}
