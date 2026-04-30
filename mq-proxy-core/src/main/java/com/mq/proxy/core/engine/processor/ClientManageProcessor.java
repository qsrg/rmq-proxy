package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import com.mq.proxy.core.protocol.header.UnregisterClientRequestHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ClientManageProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(ClientManageProcessor.class);

    private final ClientConnectionManager clientConnectionManager;

    public ClientManageProcessor(ClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        int requestCode = request.getCode();

        if (requestCode == RequestCode.HEART_BEAT) {
            return heartBeat(channel, request);
        } else if (requestCode == RequestCode.UNREGISTER_CLIENT) {
            return unregisterClient(channel, request);
        } else if (requestCode == RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
            return getConsumerListByGroup(request);
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }
    }

    private RemotingCommand heartBeat(Channel channel, RemotingCommand request) {
        byte[] body = request.getBody();
        if (body != null && body.length > 0) {
            HeartbeatData heartbeatData = HeartbeatData.decode(body);
            String clientID = heartbeatData.getClientID();

            if (heartbeatData.getProducerDataSet() != null) {
                for (HeartbeatData.ProducerData producerData : heartbeatData.getProducerDataSet()) {
                    clientConnectionManager.registerProducer(channel, clientID, producerData.getGroupName());
                }
            }

            if (heartbeatData.getConsumerDataSet() != null) {
                for (HeartbeatData.ConsumerData consumerData : heartbeatData.getConsumerDataSet()) {
                    clientConnectionManager.registerConsumer(channel, clientID, consumerData.getGroupName(),
                            null, null, null, consumerData.getSubscriptionDataSet());
                }
            }
        }

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private RemotingCommand unregisterClient(Channel channel, RemotingCommand request) {
        UnregisterClientRequestHeader requestHeader = parseUnregisterClientRequestHeader(request);
        String clientID = requestHeader.getClientID();

        clientConnectionManager.unregisterClient(channel, clientID,
                requestHeader.getProducerGroup(), requestHeader.getConsumerGroup());

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private RemotingCommand getConsumerListByGroup(RemotingCommand request) {
        String consumerGroup = null;
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            consumerGroup = extFields.get("consumerGroup");
        }

        List<HeartbeatData.ConsumerData> allConsumerData = clientConnectionManager.getAllConsumerData();
        java.util.List<String> consumerIdList = new java.util.ArrayList<>();
        if (consumerGroup != null) {
            for (HeartbeatData.ConsumerData data : allConsumerData) {
                if (consumerGroup.equals(data.getGroupName())) {
                    consumerIdList.add("proxy-client-" + data.getGroupName());
                }
            }
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        StringBuilder json = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < consumerIdList.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(consumerIdList.get(i)).append("\"");
        }
        json.append("]}");
        response.setBody(json.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return response;
    }

    private UnregisterClientRequestHeader parseUnregisterClientRequestHeader(RemotingCommand request) {
        UnregisterClientRequestHeader header = (UnregisterClientRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new UnregisterClientRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setClientID(extFields.get("clientID"));
            header.setProducerGroup(extFields.get("producerGroup"));
            header.setConsumerGroup(extFields.get("consumerGroup"));
        }
        return header;
    }

    public ClientConnectionManager getClientConnectionManager() {
        return clientConnectionManager;
    }
}
