package com.mq.proxy.core.protocol.body;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;

public class UnlockBatchRequestBody {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String consumerGroup;
    private String clientId;
    private Set<MessageQueue> mqSet = new HashSet<MessageQueue>();

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Set<MessageQueue> getMqSet() {
        return mqSet;
    }

    public void setMqSet(Set<MessageQueue> mqSet) {
        this.mqSet = mqSet;
    }

    public static UnlockBatchRequestBody decode(byte[] body) throws Exception {
        return objectMapper.readValue(body, UnlockBatchRequestBody.class);
    }

    public byte[] encode() throws Exception {
        return objectMapper.writeValueAsBytes(this);
    }
}