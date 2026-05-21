package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class QueryMessageRequestHeader implements CommandCustomHeader {
    private String topic;
    private String key;
    private Integer maxNum;
    private Long beginTimestamp;
    private Long endTimestamp;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (topic != null) map.put("topic", topic);
        if (key != null) map.put("key", key);
        if (maxNum != null) map.put("maxNum", String.valueOf(maxNum));
        if (beginTimestamp != null) map.put("beginTimestamp", String.valueOf(beginTimestamp));
        if (endTimestamp != null) map.put("endTimestamp", String.valueOf(endTimestamp));
        return map;
    }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    public Integer getMaxNum() { return maxNum; }
    public void setMaxNum(Integer maxNum) { this.maxNum = maxNum; }
    public Long getBeginTimestamp() { return beginTimestamp; }
    public void setBeginTimestamp(Long beginTimestamp) { this.beginTimestamp = beginTimestamp; }
    public Long getEndTimestamp() { return endTimestamp; }
    public void setEndTimestamp(Long endTimestamp) { this.endTimestamp = endTimestamp; }
}