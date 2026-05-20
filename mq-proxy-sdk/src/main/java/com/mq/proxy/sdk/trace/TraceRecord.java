package com.mq.proxy.sdk.trace;

public class TraceRecord {
    
    private String traceId;
    private String topic;
    private String msgId;
    private long sendTime;
    private boolean success;
    private String errorMsg;
    private long recordTime;
    
    public String getTraceId() {
        return traceId;
    }
    
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
    
    public String getTopic() {
        return topic;
    }
    
    public void setTopic(String topic) {
        this.topic = topic;
    }
    
    public String getMsgId() {
        return msgId;
    }
    
    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }
    
    public long getSendTime() {
        return sendTime;
    }
    
    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    public String getErrorMsg() {
        return errorMsg;
    }
    
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }
    
    public long getRecordTime() {
        return recordTime;
    }
    
    public void setRecordTime(long recordTime) {
        this.recordTime = recordTime;
    }
}
