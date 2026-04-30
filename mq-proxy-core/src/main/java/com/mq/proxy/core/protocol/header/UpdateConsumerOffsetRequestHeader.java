package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class UpdateConsumerOffsetRequestHeader implements CommandCustomHeader {
    private String consumerGroup;
    private String topic;
    private Integer queueId;
    private Long commitOffset;

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
        if (commitOffset != null) {
            map.put("commitOffset", String.valueOf(commitOffset));
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

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }
}
