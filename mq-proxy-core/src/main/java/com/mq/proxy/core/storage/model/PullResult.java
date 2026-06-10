package com.mq.proxy.core.storage.model;

import java.util.List;

public class PullResult {
    private int responseCode;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private List<InternalMessage> messageList;
    private String suggestWhichBrokerId;
    private byte[] messageBinary;

    public static PullResult notFound(long nextBeginOffset, long minOffset, long maxOffset) {
        PullResult result = new PullResult();
        result.setResponseCode(19);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(minOffset);
        result.setMaxOffset(maxOffset);
        return result;
    }

    public static PullResult fail(int responseCode, long nextBeginOffset, long minOffset, long maxOffset) {
        PullResult result = new PullResult();
        result.setResponseCode(responseCode);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(minOffset);
        result.setMaxOffset(maxOffset);
        return result;
    }

    public static PullResult found(List<InternalMessage> messages, long nextBeginOffset, long minOffset, long maxOffset) {
        PullResult result = new PullResult();
        result.setResponseCode(0);
        result.setMessageList(messages);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(minOffset);
        result.setMaxOffset(maxOffset);
        return result;
    }

    public static PullResult found(byte[] messageBinary, long nextBeginOffset, long minOffset, long maxOffset) {
        PullResult result = new PullResult();
        result.setResponseCode(0);
        result.setMessageBinary(messageBinary);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(minOffset);
        result.setMaxOffset(maxOffset);
        return result;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<InternalMessage> getMessageList() {
        return messageList;
    }

    public void setMessageList(List<InternalMessage> messageList) {
        this.messageList = messageList;
    }

    public String getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }

    public void setSuggestWhichBrokerId(String suggestWhichBrokerId) {
        this.suggestWhichBrokerId = suggestWhichBrokerId;
    }

    public byte[] getMessageBinary() {
        return messageBinary;
    }

    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }
}
