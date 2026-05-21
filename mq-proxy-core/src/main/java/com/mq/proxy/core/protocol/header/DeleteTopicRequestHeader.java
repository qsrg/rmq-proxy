package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class DeleteTopicRequestHeader implements CommandCustomHeader {
    private String topic;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (topic != null) map.put("topic", topic);
        return map;
    }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
}