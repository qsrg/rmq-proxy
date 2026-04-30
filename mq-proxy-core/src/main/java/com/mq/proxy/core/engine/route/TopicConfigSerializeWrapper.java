package com.mq.proxy.core.engine.route;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class TopicConfigSerializeWrapper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Map<String, TopicConfig> topicConfigTable = new HashMap<>();

    public Map<String, TopicConfig> getTopicConfigTable() {
        return topicConfigTable;
    }

    public void setTopicConfigTable(Map<String, TopicConfig> topicConfigTable) {
        this.topicConfigTable = topicConfigTable;
    }

    public byte[] encode() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("encode TopicConfigSerializeWrapper error", e);
        }
    }

    public static TopicConfigSerializeWrapper decode(byte[] data) {
        try {
            return MAPPER.readValue(data, TopicConfigSerializeWrapper.class);
        } catch (Exception e) {
            throw new RuntimeException("decode TopicConfigSerializeWrapper error", e);
        }
    }

    public static class TopicConfig {
        private String topicName;
        private int readQueueNums = 4;
        private int writeQueueNums = 4;
        private int perm = 6;

        public TopicConfig() {
        }

        public TopicConfig(String topicName) {
            this.topicName = topicName;
        }

        public String getTopicName() {
            return topicName;
        }

        public void setTopicName(String topicName) {
            this.topicName = topicName;
        }

        public int getReadQueueNums() {
            return readQueueNums;
        }

        public void setReadQueueNums(int readQueueNums) {
            this.readQueueNums = readQueueNums;
        }

        public int getWriteQueueNums() {
            return writeQueueNums;
        }

        public void setWriteQueueNums(int writeQueueNums) {
            this.writeQueueNums = writeQueueNums;
        }

        public int getPerm() {
            return perm;
        }

        public void setPerm(int perm) {
            this.perm = perm;
        }
    }
}
