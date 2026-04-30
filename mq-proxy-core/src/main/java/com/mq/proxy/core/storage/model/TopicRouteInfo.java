package com.mq.proxy.core.storage.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicRouteInfo {
    private String topic;
    private String orderTopicConf;
    private List<QueueData> queueDatas = new ArrayList<>();
    private List<BrokerData> brokerDatas = new ArrayList<>();
    private HashMap<String, List<String>> filterServerTable = new HashMap<>();

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }

    public static class QueueData {
        private String brokerName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;
        private int topicSysFlag;

        public String getBrokerName() {
            return brokerName;
        }

        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
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

        public int getTopicSysFlag() {
            return topicSysFlag;
        }

        public void setTopicSysFlag(int topicSysFlag) {
            this.topicSysFlag = topicSysFlag;
        }
    }

    public static class BrokerData {
        private String cluster;
        private String brokerName;
        private Map<Long, String> brokerAddrs = new HashMap<>();

        public String getCluster() {
            return cluster;
        }

        public void setCluster(String cluster) {
            this.cluster = cluster;
        }

        public String getBrokerName() {
            return brokerName;
        }

        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
        }

        public Map<Long, String> getBrokerAddrs() {
            return brokerAddrs;
        }

        public void setBrokerAddrs(Map<Long, String> brokerAddrs) {
            this.brokerAddrs = brokerAddrs;
        }
    }
}