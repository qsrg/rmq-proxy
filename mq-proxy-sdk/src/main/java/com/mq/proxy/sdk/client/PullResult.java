package com.mq.proxy.sdk.client;

import java.util.List;

public class PullResult {
    
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private List<byte[]> messageList;
    
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
    
    public List<byte[]> getMessageList() {
        return messageList;
    }
    
    public void setMessageList(List<byte[]> messageList) {
        this.messageList = messageList;
    }
}
