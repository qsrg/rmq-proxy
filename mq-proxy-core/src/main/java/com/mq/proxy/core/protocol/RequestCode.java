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
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    public static final int GET_MAX_OFFSET = 30;
    public static final int GET_MIN_OFFSET = 31;
    public static final int GET_EARLIEST_MSG_STORETIME = 32;
    public static final int VIEW_MESSAGE_BY_ID = 33;
    public static final int HEART_BEAT = 34;
    public static final int UNREGISTER_CLIENT = 35;
    public static final int CONSUMER_SEND_MSG_BACK = 36;
    public static final int END_TRANSACTION = 37;
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;
    public static final int LOCK_BATCH_MQ = 41;
    public static final int UNLOCK_BATCH_MQ = 42;
    public static final int GET_ALL_CONSUMER_OFFSET = 43;
    public static final int GET_ALL_DELAY_OFFSET = 45;

    // Broker config & runtime
    public static final int UPDATE_BROKER_CONFIG = 25;
    public static final int GET_BROKER_CONFIG = 26;
    public static final int GET_BROKER_RUNTIME_INFO = 28;

    // Consumer ops & subscription group
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    public static final int GET_TOPIC_STATS_INFO = 202;
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    public static final int GET_CONSUME_STATS = 208;
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;
    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;
    public static final int QUERY_CONSUME_TIME_SPAN = 303;
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;
    public static final int GET_CONSUMER_RUNNING_INFO = 307;
    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;
    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;
    public static final int CLONE_GROUP_OFFSET = 314;
    public static final int GET_BROKER_CONSUME_STATS = 317;

    public static final int SEND_MESSAGE_V2 = 310;
    public static final int SEND_BATCH_MESSAGE = 320;

    public static final int PUT_KV_CONFIG = 100;
    public static final int GET_KV_CONFIG = 101;
    public static final int DELETE_KV_CONFIG = 102;
    public static final int REGISTER_BROKER = 103;
    public static final int UNREGISTER_BROKER = 104;
    public static final int GET_ROUTEINFO_BY_TOPIC = 105;
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    public static final int DELETE_TOPIC_IN_BROKER = 215;
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
}