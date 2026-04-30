package com.mq.proxy.rocketmq.adapter;

import com.mq.proxy.core.storage.model.InternalMessage;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RocketMQMessageDecoder {

    public static List<InternalMessage> decodeMessages(byte[] body) {
        List<InternalMessage> messages = new ArrayList<>();
        if (body == null || body.length == 0) {
            return messages;
        }

        ByteBuffer buffer = ByteBuffer.wrap(body);
        while (buffer.remaining() > 4) {
            int msgLen = buffer.getInt();
            if (msgLen <= 0 || buffer.remaining() < msgLen) {
                break;
            }

            int startPos = buffer.position();

            buffer.getInt();
            buffer.getInt();
            buffer.getInt();

            int queueId = buffer.getInt();
            long queueOffset = buffer.getLong();

            int sysFlag = buffer.getInt();
            long bornTimestamp = buffer.getLong();

            int bornHostLength = (sysFlag & 0x01) == 0x01 ? 16 : 4;
            buffer.position(buffer.position() + bornHostLength + 8);

            int storeHostLength = (sysFlag & 0x01) == 0x01 ? 16 : 4;
            buffer.position(buffer.position() + storeHostLength + 8);

            int reconsumeTimes = buffer.getInt();
            buffer.getLong();

            int bodyLength = buffer.getInt();
            byte[] msgBody = new byte[bodyLength];
            buffer.get(msgBody);

            byte topicLength = buffer.get();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topic = new String(topicBytes, StandardCharsets.UTF_8);

            short propertiesLength = buffer.getShort();
            String properties = null;
            if (propertiesLength > 0) {
                byte[] propertiesBytes = new byte[propertiesLength];
                buffer.get(propertiesBytes);
                properties = new String(propertiesBytes, StandardCharsets.UTF_8);
            }

            buffer.position(startPos + msgLen);

            InternalMessage message = new InternalMessage();
            message.setTopic(topic);
            message.setQueueId(queueId);
            message.setBody(msgBody);
            message.setProperties(properties);
            message.setSysFlag(sysFlag);
            message.setBornTimestamp(bornTimestamp);
            message.setReconsumeTimes(reconsumeTimes);
            messages.add(message);
        }

        return messages;
    }
}
