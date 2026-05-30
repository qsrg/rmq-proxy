package com.mq.proxy.core.protocol.heartbeat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HeartbeatData {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private String clientID;
    private Set<ProducerData> producerDataSet = new HashSet<>();
    private Set<ConsumerData> consumerDataSet = new HashSet<>();

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }

    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }

    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }

    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }

    public byte[] encode() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static HeartbeatData decode(byte[] data) {
        try {
            return MAPPER.readValue(data, HeartbeatData.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ProducerData {
        private String groupName;

        public String getGroupName() {
            return groupName;
        }

        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProducerData that = (ProducerData) o;
            if (groupName != null ? !groupName.equals(that.groupName) : that.groupName != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return groupName != null ? groupName.hashCode() : 0;
        }
    }

    public static class ConsumerData {
        private String groupName;
        private Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
        private String consumeType;
        private String messageModel;
        private String consumeFromWhere;
        private boolean unitMode;

        public String getGroupName() {
            return groupName;
        }

        public void setGroupName(String groupName) {
            this.groupName = groupName;
        }

        public Set<SubscriptionData> getSubscriptionDataSet() {
            return subscriptionDataSet;
        }

        public void setSubscriptionDataSet(Set<SubscriptionData> subscriptionDataSet) {
            this.subscriptionDataSet = subscriptionDataSet;
        }

        public String getConsumeType() {
            return consumeType;
        }

        public void setConsumeType(String consumeType) {
            this.consumeType = consumeType;
        }

        public String getMessageModel() {
            return messageModel;
        }

        public void setMessageModel(String messageModel) {
            this.messageModel = messageModel;
        }

        public String getConsumeFromWhere() {
            return consumeFromWhere;
        }

        public void setConsumeFromWhere(String consumeFromWhere) {
            this.consumeFromWhere = consumeFromWhere;
        }

        public boolean isUnitMode() {
            return unitMode;
        }

        public void setUnitMode(boolean unitMode) {
            this.unitMode = unitMode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConsumerData that = (ConsumerData) o;
            if (groupName != null ? !groupName.equals(that.groupName) : that.groupName != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            return groupName != null ? groupName.hashCode() : 0;
        }
    }

    public static class SubscriptionData {
        private String topic;
        private String subString;
        private long subVersion;
        private Set<String> tagsSet = new HashSet<>();
        private Set<Integer> codeSet = new HashSet<>();
        private boolean classFilterMode;
        private String expressionType;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getSubString() {
            return subString;
        }

        public void setSubString(String subString) {
            this.subString = subString;
        }

        public long getSubVersion() {
            return subVersion;
        }

        public void setSubVersion(long subVersion) {
            this.subVersion = subVersion;
        }

        public Set<String> getTagsSet() {
            return tagsSet;
        }

        public void setTagsSet(Set<String> tagsSet) {
            this.tagsSet = tagsSet;
        }

        public Set<Integer> getCodeSet() {
            return codeSet;
        }

        public void setCodeSet(Set<Integer> codeSet) {
            this.codeSet = codeSet;
        }

        public boolean isClassFilterMode() {
            return classFilterMode;
        }

        public void setClassFilterMode(boolean classFilterMode) {
            this.classFilterMode = classFilterMode;
        }

        public String getExpressionType() {
            return expressionType;
        }

        public void setExpressionType(String expressionType) {
            this.expressionType = expressionType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SubscriptionData that = (SubscriptionData) o;
            if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
            if (subString != null ? !subString.equals(that.subString) : that.subString != null) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = topic != null ? topic.hashCode() : 0;
            result = 31 * result + (subString != null ? subString.hashCode() : 0);
            return result;
        }
    }
}
