package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.processor.AdminBrokerProcessor;
import com.mq.proxy.core.engine.processor.ClientManageProcessor;
import com.mq.proxy.core.engine.processor.ConsumerManageProcessor;
import com.mq.proxy.core.engine.processor.ConsumerSendMsgBackProcessor;
import com.mq.proxy.core.engine.processor.LockBatchMQProcessor;
import com.mq.proxy.core.engine.processor.NameServerProcessor;
import com.mq.proxy.core.engine.processor.PullMessageProcessor;
import com.mq.proxy.core.engine.processor.SendMessageProcessor;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.server.NettyRemotingServer;

public class ProcessorRegister {

    public static void registerProcessors(NettyRemotingServer remotingServer, MessageEngine messageEngine,
                                          VirtualRouteManager virtualRouteManager, ClientConnectionManager clientConnectionManager) {
        SendMessageProcessor sendMessageProcessor = new SendMessageProcessor(messageEngine);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendMessageProcessor);
        remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendMessageProcessor);
        remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendMessageProcessor);

        PullMessageProcessor pullMessageProcessor = new PullMessageProcessor(messageEngine);
        pullMessageProcessor.setVirtualRouteManager(virtualRouteManager);
        remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, pullMessageProcessor);

        ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(messageEngine);
        consumerManageProcessor.setVirtualRouteManager(virtualRouteManager);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor);

        ClientManageProcessor clientManageProcessor = new ClientManageProcessor(clientConnectionManager);
        remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientManageProcessor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientManageProcessor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, clientManageProcessor);

        ConsumerSendMsgBackProcessor consumerSendMsgBackProcessor = new ConsumerSendMsgBackProcessor(messageEngine);
        consumerSendMsgBackProcessor.setVirtualRouteManager(virtualRouteManager);
        remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, consumerSendMsgBackProcessor);

        LockBatchMQProcessor lockBatchMQProcessor = new LockBatchMQProcessor(clientConnectionManager);
        remotingServer.registerProcessor(RequestCode.LOCK_BATCH_MQ, lockBatchMQProcessor);
        remotingServer.registerProcessor(RequestCode.UNLOCK_BATCH_MQ, lockBatchMQProcessor);

        AdminBrokerProcessor adminBrokerProcessor = new AdminBrokerProcessor(messageEngine);
        adminBrokerProcessor.setVirtualRouteManager(virtualRouteManager);
        remotingServer.registerProcessor(RequestCode.GET_MAX_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_MIN_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_EARLIEST_MSG_STORETIME, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_CONSUMER_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_DELAY_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.QUERY_BROKER_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_AND_CREATE_TOPIC, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_TOPIC_CONFIG, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_TOPIC_CONFIG_LIST, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_TOPIC_NAME_LIST, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.DELETE_TOPIC_IN_BROKER, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_BROKER_CONFIG, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_BROKER_CONFIG, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_BROKER_RUNTIME_INFO, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_TOPIC_STATS_INFO, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_CONNECTION_LIST, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_PRODUCER_CONNECTION_LIST, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.DELETE_SUBSCRIPTIONGROUP, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUME_STATS, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.RESET_CONSUMER_OFFSET_IN_BROKER, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.QUERY_CONSUME_TIME_SPAN, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.CLONE_GROUP_OFFSET, adminBrokerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_BROKER_CONSUME_STATS, adminBrokerProcessor);

        NameServerProcessor nameServerProcessor = new NameServerProcessor(virtualRouteManager);
        remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.REGISTER_BROKER, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.UNREGISTER_BROKER, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_BROKER_CLUSTER_INFO, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.DELETE_TOPIC_IN_NAMESRV, nameServerProcessor);

        remotingServer.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, clientManageProcessor);
    }
}
