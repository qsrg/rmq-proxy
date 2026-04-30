package com.mq.proxy.core.engine;

import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageAdapterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ProxyBrokerHeartbeatService {

    private static final Logger log = LoggerFactory.getLogger(ProxyBrokerHeartbeatService.class);

    private static final long HEARTBEAT_INTERVAL_MILLIS = 30000;
    private static final String PROXY_CLIENT_ID_PREFIX = "MQProxy@";

    private final ClientConnectionManager clientConnectionManager;
    private final StorageAdapterManager storageAdapterManager;
    private final String proxyClientId;
    private ScheduledExecutorService scheduledExecutor;

    public ProxyBrokerHeartbeatService(ClientConnectionManager clientConnectionManager,
                                       StorageAdapterManager storageAdapterManager, String proxyHost, int proxyPort) {
        this.clientConnectionManager = clientConnectionManager;
        this.storageAdapterManager = storageAdapterManager;
        this.proxyClientId = PROXY_CLIENT_ID_PREFIX + proxyHost + "@" + proxyPort + "@" + System.currentTimeMillis();
    }

    public void start() {
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ProxyBrokerHeartbeatService.this.sendHeartbeatToAllBrokers();
                } catch (Throwable e) {
                    log.error("sendHeartbeatToAllBrokers exception", e);
                }
            }
        }, 5000, HEARTBEAT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        log.info("ProxyBrokerHeartbeatService started, clientId={}, interval={}ms", proxyClientId, HEARTBEAT_INTERVAL_MILLIS);
    }

    public void shutdown() {
        if (this.scheduledExecutor != null) {
            this.scheduledExecutor.shutdown();
        }
        log.info("ProxyBrokerHeartbeatService shutdown");
    }

    private void sendHeartbeatToAllBrokers() {
        StorageAdapter defaultAdapter = storageAdapterManager.getAdapterByTopic(null);
        if (defaultAdapter == null) {
            return;
        }

        HeartbeatData heartbeatData = buildHeartbeatData();
        if (heartbeatData == null) {
            return;
        }

        byte[] body = heartbeatData.encode();

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(body);

        try {
            RemotingCommand response = defaultAdapter.forwardToBroker(request);
            if (response != null && response.getCode() == 0) {
                log.debug("Proxy heartbeat to broker success, producerGroups={}, consumerGroups={}",
                        heartbeatData.getProducerDataSet().size(),
                        heartbeatData.getConsumerDataSet().size());
            } else {
                log.warn("Proxy heartbeat to broker failed, responseCode={}",
                        response != null ? response.getCode() : "null");
            }
        } catch (Exception e) {
            log.warn("Proxy heartbeat to broker exception: {}", e.getMessage());
        }
    }

    private HeartbeatData buildHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(proxyClientId);

        List<HeartbeatData.ProducerData> producerDataList = clientConnectionManager.getAllProducerData();
        for (HeartbeatData.ProducerData pd : producerDataList) {
            heartbeatData.getProducerDataSet().add(pd);
        }

        List<HeartbeatData.ConsumerData> consumerDataList = clientConnectionManager.getAllConsumerData();
        for (HeartbeatData.ConsumerData cd : consumerDataList) {
            heartbeatData.getConsumerDataSet().add(cd);
        }

        return heartbeatData;
    }

    public String getProxyClientId() {
        return proxyClientId;
    }
}
