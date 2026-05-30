package com.mq.proxy.sdk.consumer.model;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class DecodedMessage {

    private String topic;
    private int queueId;
    private long queueOffset;
    private byte[] body;
    private int flag;
    private int sysFlag;
    private long bornTimestamp;
    private long storeTimestamp;
    private int reconsumeTimes;
    private String properties;

    public DecodedMessage() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    @Override
    public String toString() {
        return "DecodedMessage{topic='" + topic + "', queueId=" + queueId +
                ", queueOffset=" + queueOffset + ", bodySize=" + (body != null ? body.length : 0) + '}';
    }

    public static List<DecodedMessage> decode(byte[] data) {
        List<DecodedMessage> msgList = new ArrayList<>();
        if (data == null || data.length == 0) {
            return msgList;
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);

        while (buffer.remaining() > 4) {
            int startPos = buffer.position();
            int totalSize = buffer.getInt();

            if (totalSize <= 4 || startPos + totalSize > buffer.limit()) {
                break;
            }

            try {
                DecodedMessage msg = decodeOneMessage(buffer);
                if (msg != null) {
                    msgList.add(msg);
                }
            } catch (Exception e) {
                break;
            } finally {
                buffer.position(startPos + totalSize);
            }
        }

        return msgList;
    }

    private static DecodedMessage decodeOneMessage(ByteBuffer buffer) {
        DecodedMessage msg = new DecodedMessage();

        buffer.getInt();
        buffer.getInt();
        msg.setQueueId(buffer.getInt());
        msg.setFlag(buffer.getInt());
        msg.setQueueOffset(buffer.getLong());
        buffer.getLong();
        msg.setSysFlag(buffer.getInt());
        msg.setBornTimestamp(buffer.getLong());

        int bornHostIPLen = (msg.getSysFlag() & 0x01) == 0x01 ? 16 : 4;
        buffer.position(buffer.position() + bornHostIPLen);
        buffer.getInt();

        msg.setStoreTimestamp(buffer.getLong());

        int storeHostIPLen = (msg.getSysFlag() & 0x01) == 0x01 ? 16 : 4;
        buffer.position(buffer.position() + storeHostIPLen);
        buffer.getInt();

        msg.setReconsumeTimes(buffer.getInt());
        buffer.getLong();

        int bodyLength = buffer.getInt();
        if (bodyLength > 0) {
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            msg.setBody(body);
        }

        byte topicLength = buffer.get();
        if (topicLength > 0) {
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            msg.setTopic(new String(topicBytes, StandardCharsets.UTF_8));
        }

        short propertiesLength = buffer.getShort();
        if (propertiesLength > 0) {
            byte[] propertiesBytes = new byte[propertiesLength];
            buffer.get(propertiesBytes);
            msg.setProperties(new String(propertiesBytes, StandardCharsets.UTF_8));
        }

        return msg;
    }
}
