package com.mq.proxy.sdk.consumer;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageRequestHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;
import com.mq.proxy.sdk.remoting.SDKRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Proxy消费者 - 简化API，类似RocketMQ使用方式
 *
 * <pre>
 * // 简单使用（类似RocketMQ）
 * ProxyConsumer consumer = new ProxyConsumer("ConsumerGroup");
 * consumer.setProxyAddrs("127.0.0.1:10911");
 * consumer.start();
 * PullResult result = consumer.pull("Topic", "ConsumerGroup", 0, 0L, 32);
 * consumer.shutdown();
 *
 * // 链式配置
 * ProxyConsumer consumer = new ProxyConsumer("ConsumerGroup")
 *     .setProxyAddrs("127.0.0.1:10911")
 *     .setSuspendTimeoutMillis(15000); // 长轮询
 * consumer.start();
 * </pre>
 */
public class ProxyConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProxyConsumer.class);

    private static final int FLAG_SUSPEND = 0x02;
    private static final int QUERY_NOT_FOUND = 206;

    private final ProxyConsumerConfig config;
    private ProxyClientFacade facade; // 延迟初始化（start时创建）

    private volatile boolean started = false;

    private final LocalOffsetStore localOffsetStore = new LocalOffsetStore();

    private final CopyOnWriteArrayList<ConsumerChangeListener> consumerChangeListeners =
        new CopyOnWriteArrayList<>();

    /**
     * 简化构造函数 - 推荐使用方式（类似RocketMQ）
     *
     * @param consumerGroup 消费者组名
     */
    public ProxyConsumer(String consumerGroup) {
        this.config = new ProxyConsumerConfig();
        this.config.setConsumerGroup(consumerGroup);
        this.facade = null; // 延迟初始化
    }

    /**
     * 完整配置构造函数 - 高级配置场景
     *
     * @param config 消费者配置
     */
    public ProxyConsumer(ProxyConsumerConfig config) {
        this.config = config;
        this.facade = null; // 延迟初始化
    }

    // ========== 链式配置方法（推荐使用） ==========

    public ProxyConsumer setProxyAddrs(String proxyAddrs) {
        this.config.setProxyAddrs(proxyAddrs);
        return this;
    }

    public ProxyConsumer setSuspendTimeoutMillis(long suspendTimeoutMillis) {
        this.config.setSuspendTimeoutMillis(suspendTimeoutMillis);
        return this;
    }

    public ProxyConsumer setMessageModel(String messageModel) {
        this.config.setMessageModel(messageModel);
        return this;
    }

    public ProxyConsumer setRequestTimeoutMillis(int timeoutMillis) {
        this.config.setRequestTimeoutMillis(timeoutMillis);
        return this;
    }

    public ProxyConsumer setRetryTimes(int retryTimes) {
        this.config.setRetryTimes(retryTimes);
        return this;
    }

    public ProxyConsumer setEnableMetrics(boolean enableMetrics) {
        this.config.setEnableMetrics(enableMetrics);
        return this;
    }

    public ProxyConsumer setEnableTrace(boolean enableTrace) {
        this.config.setEnableTrace(enableTrace);
        return this;
    }

    public ProxyConsumer setTlsEnabled(boolean tlsEnabled) {
        this.config.setTlsEnabled(tlsEnabled);
        return this;
    }

    // ========== 启动和关闭 ==========

    public void start() {
        if (!started) {
            if (facade == null) {
                facade = new ProxyClientFacade(config);
            }
            facade.start();
            registerNotifyProcessor();
            sendConsumerRegistration();
            started = true;
            log.info("ProxyConsumer started, group={}, proxyAddrs={}, messageModel={}",
                config.getConsumerGroup(), config.getProxyAddrs(), config.getMessageModel());
        }
    }

    public void shutdown() {
        if (started) {
            if (facade != null) {
                facade.shutdown();
            }
            started = false;
            log.info("ProxyConsumer shutdown, group={}", config.getConsumerGroup());
        }
    }

    // ========== 消费消息 ==========

    public void registerConsumerChangeListener(ConsumerChangeListener listener) {
        consumerChangeListeners.add(listener);
    }

    public PullResult pull(String topic, String consumerGroup,
                          int queueId, long offset, int maxNums) throws ProxyException {

        ensureStarted();

        try {
            RemotingCommand request = buildPullMessageRequest(topic, consumerGroup, queueId, offset, maxNums);

            long pullTimeout = config.getRequestTimeoutMillis();
            long suspendTimeout = config.getSuspendTimeoutMillis();
            if (suspendTimeout > 0) {
                pullTimeout = suspendTimeout + config.getRequestTimeoutMillis();
            }

            RemotingCommand response = facade.invokeSync(request, pullTimeout);

            return parsePullResult(response);

        } catch (Exception e) {
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Pull message failed", e);
        }
    }

    public long queryConsumerOffset(String consumerGroup, String topic, int queueId) throws ProxyException {
        ensureStarted();

        if ("BROADCASTING".equals(config.getMessageModel())) {
            long localOffset = localOffsetStore.getOffset(consumerGroup, topic, queueId);
            if (localOffset >= 0) {
                return localOffset;
            }
            return -1L;
        }

        QueryConsumerOffsetRequestHeader header = new QueryConsumerOffsetRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, header);
        request.makeCustomHeaderToNet();

        try {
            RemotingCommand response = facade.invokeSync(request, config.getRequestTimeoutMillis());

            if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
                HashMap<String, String> extFields = response.getExtFields();
                if (extFields != null && extFields.get("offset") != null) {
                    return Long.parseLong(extFields.get("offset"));
                }
                return -1L;
            }
            if (response.getCode() == QUERY_NOT_FOUND) {
                return -1L;
            }
            throw new ProxyException("Query consumer offset failed, code=" + response.getCode());
        } catch (Exception e) {
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Query consumer offset failed", e);
        }
    }

    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset)
            throws ProxyException {
        ensureStarted();

        if ("BROADCASTING".equals(config.getMessageModel())) {
            localOffsetStore.updateOffset(consumerGroup, topic, queueId, commitOffset);
            return;
        }

        UpdateConsumerOffsetRequestHeader header = new UpdateConsumerOffsetRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setCommitOffset(commitOffset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, header);
        request.makeCustomHeaderToNet();

        try {
            RemotingCommand response = facade.invokeSync(request, config.getRequestTimeoutMillis());

            if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
                throw new ProxyException("Update consumer offset failed, code=" + response.getCode());
            }
        } catch (Exception e) {
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Update consumer offset failed", e);
        }
    }

    // ========== 监控 ==========

    public Map<String, ProxyMetricsSnapshot> getMetrics() {
        ensureStarted();
        return facade.getMetricsCollector().getSnapshot();
    }

    public ProxyConsumerConfig getConfig() {
        return config;
    }

    // ========== 私有方法 ==========

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("ProxyConsumer not started, call start() first");
        }
        if (facade == null) {
            throw new IllegalStateException("ProxyConsumer facade not initialized");
        }
    }

    private void sendConsumerRegistration() {
        String consumerGroup = config.getConsumerGroup();
        if (consumerGroup == null || consumerGroup.isEmpty()) {
            return;
        }
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID("SDK@" + consumerGroup + "@" + System.currentTimeMillis());

        HeartbeatData.ConsumerData consumerData = new HeartbeatData.ConsumerData();
        consumerData.setGroupName(consumerGroup);
        consumerData.setMessageModel(config.getMessageModel());
        consumerData.setConsumeType("CONSUME_ACTIVELY");
        consumerData.setConsumeFromWhere("CONSUME_FROM_LAST_OFFSET");
        heartbeatData.getConsumerDataSet().add(consumerData);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeatData.encode());

        try {
            RemotingCommand response = facade.invokeSync(request, config.getRequestTimeoutMillis());
            if (response != null && response.getCode() == RemotingSysResponseCode.SUCCESS) {
                log.info("SDK consumer registration sent, group={}, messageModel={}",
                        consumerGroup, config.getMessageModel());
            }
        } catch (Exception e) {
            log.warn("SDK consumer registration failed (non-critical): {}", e.getMessage());
        }
    }

    private void registerNotifyProcessor() {
        facade.getRemotingClient().registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
            new SDKRequestProcessor() {
                @Override
                public void processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                    HashMap<String, String> extFields = request.getExtFields();
                    String group = extFields != null ? extFields.get("consumerGroup") : null;
                    if (group != null) {
                        log.info("Received NOTIFY_CONSUMER_IDS_CHANGED for group {}", group);
                        for (ConsumerChangeListener listener : consumerChangeListeners) {
                            listener.onConsumerIdsChanged(group);
                        }
                    }
                }
            });
    }

    private RemotingCommand buildPullMessageRequest(String topic, String consumerGroup,
                                                    int queueId, long offset, int maxNums) {
        long suspendTimeout = config.getSuspendTimeoutMillis();
        int sysFlag = suspendTimeout > 0 ? FLAG_SUSPEND : 0;

        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setQueueOffset(offset);
        header.setMaxMsgNums(maxNums);
        header.setSysFlag(sysFlag);
        header.setCommitOffset(0L);
        header.setSuspendTimeoutMillis(suspendTimeout);
        header.setSubscription("*");
        header.setSubVersion(System.currentTimeMillis());
        header.setExpressionType("TAG");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
        request.makeCustomHeaderToNet();

        return request;
    }

    private PullResult parsePullResult(RemotingCommand response) {
        PullResult result = new PullResult();
        HashMap<String, String> extFields = response.getExtFields();

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            result.setSuccess(true);
            result.setFound(true);
            result.setResponseCode(response.getCode());
            parsePullExtFields(result, extFields);
            if (extFields != null && extFields.get("suggestWhichBrokerId") != null) {
                result.setSuggestWhichBrokerId(Long.parseLong(extFields.get("suggestWhichBrokerId")));
            }
            result.setBody(response.getBody());
        } else if (response.getCode() == ResponseCode.PULL_NOT_FOUND) {
            result.setSuccess(true);
            result.setFound(false);
            result.setResponseCode(response.getCode());
            parsePullExtFields(result, extFields);
        } else {
            result.setSuccess(false);
            result.setFound(false);
            result.setResponseCode(response.getCode());
            result.setErrorMsg(response.getRemark() != null ? response.getRemark() :
                "Pull failed with code=" + response.getCode());
        }

        return result;
    }

    private void parsePullExtFields(PullResult result, HashMap<String, String> extFields) {
        if (extFields != null) {
            if (extFields.get("nextBeginOffset") != null) {
                result.setNextBeginOffset(Long.parseLong(extFields.get("nextBeginOffset")));
            }
            if (extFields.get("minOffset") != null) {
                result.setMinOffset(Long.parseLong(extFields.get("minOffset")));
            }
            if (extFields.get("maxOffset") != null) {
                result.setMaxOffset(Long.parseLong(extFields.get("maxOffset")));
            }
            if (extFields.get("suggestWhichBrokerId") != null) {
                result.setSuggestWhichBrokerId(Long.parseLong(extFields.get("suggestWhichBrokerId")));
            }
        }
    }

    private static class LocalOffsetStore {
        private final ConcurrentHashMap<String, Long> offsetTable = new ConcurrentHashMap<>();

        private String key(String group, String topic, int queueId) {
            return group + "@" + topic + "@" + queueId;
        }

        public long getOffset(String group, String topic, int queueId) {
            return offsetTable.getOrDefault(key(group, topic, queueId), -1L);
        }

        public void updateOffset(String group, String topic, int queueId, long offset) {
            offsetTable.put(key(group, topic, queueId), offset);
        }
    }
}
