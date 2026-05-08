package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class SendMessageRequestHeader implements CommandCustomHeader {
    private String producerGroup;
    private String topic;
    private String defaultTopic;
    private Integer defaultTopicQueueNums;
    private Integer queueId;
    private Integer sysFlag;
    private Long bornTimestamp;
    private Integer flag;
    private String properties;
    private Integer reconsumeTimes;
    private boolean unitMode = false;
    private boolean batch = false;
    private Integer maxReconsumeTimes;
    private Integer topicSysFlag = 0;
    private String brokerName;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (producerGroup != null) {
            map.put("producerGroup", producerGroup);
        }
        if (topic != null) {
            map.put("topic", topic);
        }
        if (defaultTopic != null) {
            map.put("defaultTopic", defaultTopic);
        }
        if (defaultTopicQueueNums != null) {
            map.put("defaultTopicQueueNums", String.valueOf(defaultTopicQueueNums));
        }
        if (queueId != null) {
            map.put("queueId", String.valueOf(queueId));
        }
        if (sysFlag != null) {
            map.put("sysFlag", String.valueOf(sysFlag));
        }
        if (bornTimestamp != null) {
            map.put("bornTimestamp", String.valueOf(bornTimestamp));
        }
        if (flag != null) {
            map.put("flag", String.valueOf(flag));
        }
        if (properties != null) {
            map.put("properties", properties);
        }
        if (reconsumeTimes != null) {
            map.put("reconsumeTimes", String.valueOf(reconsumeTimes));
        }
        map.put("unitMode", String.valueOf(unitMode));
        map.put("batch", String.valueOf(batch));
        if (maxReconsumeTimes != null) {
            map.put("maxReconsumeTimes", String.valueOf(maxReconsumeTimes));
        }
        if (topicSysFlag != null) {
            map.put("topicSysFlag", String.valueOf(topicSysFlag));
        }
        if (brokerName != null) {
            map.put("brokerName", brokerName);
        }
        return map;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public Integer getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(Integer topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
