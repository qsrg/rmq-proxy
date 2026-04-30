package com.mq.proxy.core.storage.model;

public class PutResult {
    private boolean success;
    private String msgId;
    private int queueId;
    private long queueOffset;
    private String transactionId;
    private int responseCode;
    private String remark;

    public static PutResult success(String msgId, int queueId, long queueOffset) {
        PutResult result = new PutResult();
        result.setSuccess(true);
        result.setMsgId(msgId);
        result.setQueueId(queueId);
        result.setQueueOffset(queueOffset);
        result.setResponseCode(0);
        return result;
    }

    public static PutResult fail(int responseCode, String remark) {
        PutResult result = new PutResult();
        result.setSuccess(false);
        result.setResponseCode(responseCode);
        result.setRemark(remark);
        return result;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }
}
