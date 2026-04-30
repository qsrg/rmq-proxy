package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class SendMessageResponseHeader implements CommandCustomHeader {
    private String msgId;
    private Integer queueId;
    private Long queueOffset;
    private String transactionId;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (msgId != null) {
            map.put("msgId", msgId);
        }
        if (queueId != null) {
            map.put("queueId", String.valueOf(queueId));
        }
        if (queueOffset != null) {
            map.put("queueOffset", String.valueOf(queueOffset));
        }
        if (transactionId != null) {
            map.put("transactionId", transactionId);
        }
        return map;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
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

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
}
