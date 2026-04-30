package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class UnregisterClientRequestHeader implements CommandCustomHeader {
    private String clientID;
    private String producerGroup;
    private String consumerGroup;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (clientID != null) {
            map.put("clientID", clientID);
        }
        if (producerGroup != null) {
            map.put("producerGroup", producerGroup);
        }
        if (consumerGroup != null) {
            map.put("consumerGroup", consumerGroup);
        }
        return map;
    }

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}
