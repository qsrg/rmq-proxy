package com.mq.proxy.core.protocol.body;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Set;

public class LockBatchResponseBody {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Set<MessageQueue> lockOKMQSet = new HashSet<MessageQueue>();

    public Set<MessageQueue> getLockOKMQSet() {
        return lockOKMQSet;
    }

    public void setLockOKMQSet(Set<MessageQueue> lockOKMQSet) {
        this.lockOKMQSet = lockOKMQSet;
    }

    public static LockBatchResponseBody decode(byte[] body) throws Exception {
        return objectMapper.readValue(body, LockBatchResponseBody.class);
    }

    public byte[] encode() throws Exception {
        return objectMapper.writeValueAsBytes(this);
    }
}