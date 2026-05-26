package com.mq.proxy.sdk.client;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;
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

public class ProxyClient {

    private static final Logger log = LoggerFactory.getLogger(ProxyClient.class);

    // RocketMQ PullSysFlag: FLAG_SUSPEND = 0x1 << 1
    private static final int FLAG_SUSPEND = 0x02;

    // RocketMQ: QUERY_NOT_FOUND = 206
    private static final int QUERY_NOT_FOUND = 206;

    private final ProxyClientConfig config;
    private final ProxyClientFacade facade;

    private volatile boolean started = false;

    private final LocalOffsetStore localOffsetStore = new LocalOffsetStore();

    private final CopyOnWriteArrayList<ConsumerChangeListener> consumerChangeListeners =
        new CopyOnWriteArrayList<>();

    public ProxyClient(ProxyClientConfig config) {
        this.config = config;
        this.facade = new ProxyClientFacade(config);
    }

    ProxyClient(ProxyClientConfig config, ProxyClientFacade facade) {
        this.config = config;
        this.facade = facade;
    }

    public void start() {
        if (!started) {
            facade.start();
            registerNotifyProcessor();
            sendConsumerRegistration();
            started = true;
            log.info("ProxyClient started, proxy addresses: {}, messageModel: {}",
                    config.getProxyAddrs(), config.getMessageModel());
        }
    }

    private void sendConsumerRegistration() {
        String consumerGroup = config.getProducerGroup();
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
                    String consumerGroup = extFields != null ? extFields.get("consumerGroup") : null;
                    if (consumerGroup != null) {
                        log.info("Received NOTIFY_CONSUMER_IDS_CHANGED for group {}", consumerGroup);
                        for (ConsumerChangeListener listener : consumerChangeListeners) {
                            listener.onConsumerIdsChanged(consumerGroup);
                        }
                    }
                }
            });
    }

    public void registerConsumerChangeListener(ConsumerChangeListener listener) {
        consumerChangeListeners.add(listener);
    }

    public SendResult send(String topic, String tags, byte[] body) throws ProxyException {
        return send(topic, tags, null, body);
    }

    public SendResult send(String topic, String tags, String keys, byte[] body)
            throws ProxyException {

        String traceId = null;
        if (config.isEnableTrace()) {
            traceId = facade.getTraceCollector().generateTraceId();
        }

        long startTime = System.currentTimeMillis();

        try {
            RemotingCommand request = buildSendMessageRequest(topic, tags, keys, body, traceId);

            RemotingCommand response = facade.invokeSync(request, config.getRequestTimeoutMillis());

            SendResult result = parseSendResult(response);
            result.setTraceId(traceId);

            if (config.isEnableTrace()) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, result.getMsgId(),
                    startTime, result.isSuccess(), result.isSuccess() ? null : result.getErrorMsg());
            }

            return result;

        } catch (Exception e) {
            if (config.isEnableTrace() && traceId != null) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, null,
                    startTime, false, e.getMessage());
            }

            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Send message failed", e);
        }
    }

    public PullResult pull(String topic, String consumerGroup,
                          int queueId, long offset, int maxNums) throws ProxyException {

        try {
            RemotingCommand request = buildPullMessageRequest(topic, consumerGroup, queueId, offset, maxNums);

            // 长轮询时broker可能等待suspendTimeoutMillis才返回，
            // SDK必须等足够久才能收到broker的响应
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
        // 广播模式使用本地偏移量
        if ("BROADCASTING".equals(config.getMessageModel())) {
            long localOffset = localOffsetStore.getOffset(consumerGroup, topic, queueId);
            if (localOffset >= 0) {
                return localOffset;
            }
            // 首次消费返回-1，让消费者决定从哪里开始
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
                // 新消费者首次查询offset，broker返回206表示"从未消费过"
                // 返回-1让消费者决定从head或tail开始
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
        // 广播模式本地存储偏移量，不向broker提交
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

    public void shutdown() {
        if (started) {
            facade.shutdown();
            started = false;
            log.info("ProxyClient shutdown");
        }
    }

    public Map<String, ProxyMetricsSnapshot> getMetrics() {
        return facade.getMetricsCollector().getSnapshot();
    }

    private RemotingCommand buildSendMessageRequest(String topic, String tags, String keys,
                                                    byte[] body, String traceId) {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(config.getProducerGroup());
        header.setTopic(topic);
        header.setDefaultTopic("TBW102");
        header.setDefaultTopicQueueNums(4);
        header.setQueueId(-1);
        header.setSysFlag(0);
        header.setBornTimestamp(System.currentTimeMillis());
        header.setFlag(0);

        String properties = buildMessageProperties(tags, keys, traceId);
        header.setProperties(properties);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
        request.setBody(body);
        request.makeCustomHeaderToNet();

        return request;
    }

    private String buildMessageProperties(String tags, String keys, String traceId) {
        StringBuilder sb = new StringBuilder();
        if (tags != null && !tags.isEmpty()) {
            sb.append("TAGS").append((char) 1).append(tags);
        }
        if (keys != null && !keys.isEmpty()) {
            if (sb.length() > 0) {
                sb.append((char) 2);
            }
            sb.append("KEYS").append((char) 1).append(keys);
        }
        if (traceId != null && !traceId.isEmpty()) {
            if (sb.length() > 0) {
                sb.append((char) 2);
            }
            sb.append("TRACE_ID").append((char) 1).append(traceId);
        }
        return sb.toString();
    }

    private SendResult parseSendResult(RemotingCommand response) {
        SendResult result = new SendResult();

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            result.setSuccess(true);
            HashMap<String, String> extFields = response.getExtFields();
            if (extFields != null) {
                result.setMsgId(extFields.get("msgId"));
                if (extFields.get("queueId") != null) {
                    result.setQueueId(Integer.parseInt(extFields.get("queueId")));
                }
                if (extFields.get("queueOffset") != null) {
                    result.setQueueOffset(Long.parseLong(extFields.get("queueOffset")));
                }
            }
        } else {
            result.setSuccess(false);
            result.setErrorMsg(response.getRemark() != null ? response.getRemark() :
                "Send failed with code=" + response.getCode());
        }

        return result;
    }

    private RemotingCommand buildPullMessageRequest(String topic, String consumerGroup,
                                                    int queueId, long offset, int maxNums) {
        long suspendTimeout = config.getSuspendTimeoutMillis();
        // suspendTimeout > 0时设置SUSPEND标志位，broker才真正等待消息到达
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

    /**
     * 广播模式本地偏移量存储
     */
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