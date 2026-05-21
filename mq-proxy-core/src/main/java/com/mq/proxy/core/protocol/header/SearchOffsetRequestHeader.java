package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class SearchOffsetRequestHeader implements CommandCustomHeader {
    private String topic;
    private Integer queueId;
    private Long timestamp;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (topic != null) map.put("topic", topic);
        if (queueId != null) map.put("queueId", String.valueOf(queueId));
        if (timestamp != null) map.put("timestamp", String.valueOf(timestamp));
        return map;
    }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public Integer getQueueId() { return queueId; }
    public void setQueueId(Integer queueId) { this.queueId = queueId; }
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
}