package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.processor.ClientManageProcessor;
import com.mq.proxy.core.engine.processor.ConsumerManageProcessor;
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

        NameServerProcessor nameServerProcessor = new NameServerProcessor(virtualRouteManager);
        remotingServer.registerProcessor(RequestCode.GET_ROUTEINFO_BY_TOPIC, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.REGISTER_BROKER, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_BROKER_CLUSTER_INFO, nameServerProcessor);
        remotingServer.registerProcessor(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, nameServerProcessor);
    }
}
