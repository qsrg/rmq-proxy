package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class RegisterBrokerRequestHeader implements CommandCustomHeader {
    private String brokerName;
    private String brokerAddr;
    private String clusterName;
    private String haServerAddr;
    private Long brokerId;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (brokerName != null) {
            map.put("brokerName", brokerName);
        }
        if (brokerAddr != null) {
            map.put("brokerAddr", brokerAddr);
        }
        if (clusterName != null) {
            map.put("clusterName", clusterName);
        }
        if (haServerAddr != null) {
            map.put("haServerAddr", haServerAddr);
        }
        if (brokerId != null) {
            map.put("brokerId", String.valueOf(brokerId));
        }
        return map;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public Long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(Long brokerId) {
        this.brokerId = brokerId;
    }
}
