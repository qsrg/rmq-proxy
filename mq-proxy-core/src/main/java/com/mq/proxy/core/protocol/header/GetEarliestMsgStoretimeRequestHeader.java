package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class GetEarliestMsgStoretimeRequestHeader implements CommandCustomHeader {
    private String topic;
    private Integer queueId;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (topic != null) map.put("topic", topic);
        if (queueId != null) map.put("queueId", String.valueOf(queueId));
        return map;
    }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public Integer getQueueId() { return queueId; }
    public void setQueueId(Integer queueId) { this.queueId = queueId; }
}