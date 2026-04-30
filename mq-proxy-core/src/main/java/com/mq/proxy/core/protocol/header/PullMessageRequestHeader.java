package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class PullMessageRequestHeader implements CommandCustomHeader {
    private String consumerGroup;
    private String topic;
    private Integer queueId;
    private Long queueOffset;
    private Integer maxMsgNums;
    private Integer sysFlag;
    private Long commitOffset;
    private Long suspendTimeoutMillis;
    private String subscription;
    private Long subVersion;
    private String expressionType;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (consumerGroup != null) {
            map.put("consumerGroup", consumerGroup);
        }
        if (topic != null) {
            map.put("topic", topic);
        }
        if (queueId != null) {
            map.put("queueId", String.valueOf(queueId));
        }
        if (queueOffset != null) {
            map.put("queueOffset", String.valueOf(queueOffset));
        }
        if (maxMsgNums != null) {
            map.put("maxMsgNums", String.valueOf(maxMsgNums));
        }
        if (sysFlag != null) {
            map.put("sysFlag", String.valueOf(sysFlag));
        }
        if (commitOffset != null) {
            map.put("commitOffset", String.valueOf(commitOffset));
        }
        if (suspendTimeoutMillis != null) {
            map.put("suspendTimeoutMillis", String.valueOf(suspendTimeoutMillis));
        }
        if (subscription != null) {
            map.put("subscription", subscription);
        }
        if (subVersion != null) {
            map.put("subVersion", String.valueOf(subVersion));
        }
        if (expressionType != null) {
            map.put("expressionType", expressionType);
        }
        return map;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public Long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public Long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(Long subVersion) {
        this.subVersion = subVersion;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }
}
