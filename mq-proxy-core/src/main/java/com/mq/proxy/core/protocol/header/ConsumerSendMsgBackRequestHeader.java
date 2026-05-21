package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class ConsumerSendMsgBackRequestHeader implements CommandCustomHeader {

    private Long offset;
    private String group;
    private Integer delayLevel;
    private String originMsgId;
    private String originTopic;
    private boolean unitMode = false;
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (offset != null) {
            map.put("offset", String.valueOf(offset));
        }
        if (group != null) {
            map.put("group", group);
        }
        if (delayLevel != null) {
            map.put("delayLevel", String.valueOf(delayLevel));
        }
        if (originMsgId != null) {
            map.put("originMsgId", originMsgId);
        }
        if (originTopic != null) {
            map.put("originTopic", originTopic);
        }
        map.put("unitMode", String.valueOf(unitMode));
        if (maxReconsumeTimes != null) {
            map.put("maxReconsumeTimes", String.valueOf(maxReconsumeTimes));
        }
        return map;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getDelayLevel() {
        return delayLevel;
    }

    public void setDelayLevel(Integer delayLevel) {
        this.delayLevel = delayLevel;
    }

    public String getOriginMsgId() {
        return originMsgId;
    }

    public void setOriginMsgId(String originMsgId) {
        this.originMsgId = originMsgId;
    }

    public String getOriginTopic() {
        return originTopic;
    }

    public void setOriginTopic(String originTopic) {
        this.originTopic = originTopic;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }
}