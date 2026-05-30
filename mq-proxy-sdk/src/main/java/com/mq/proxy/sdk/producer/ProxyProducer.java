package com.mq.proxy.sdk.producer;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import com.mq.proxy.sdk.monitor.ProxyMetricsSnapshot;
import com.mq.proxy.sdk.remoting.InvokeCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Proxy生产者 - 简化API，类似RocketMQ使用方式
 *
 * <pre>
 * // 简单使用（类似RocketMQ）
 * ProxyProducer producer = new ProxyProducer("ProducerGroup");
 * producer.setProxyAddrs("127.0.0.1:19876");
 * producer.start();
 * SendResult result = producer.send("Topic", "TagA", "Hello".getBytes());
 * producer.shutdown();
 *
 * // 链式配置
 * ProxyProducer producer = new ProxyProducer("ProducerGroup")
 *     .setProxyAddrs("127.0.0.1:19876")
 *     .setRetryTimes(3)
 *     .setEnableTrace(true);
 * producer.start();
 * </pre>
 */
public class ProxyProducer {

    private static final Logger log = LoggerFactory.getLogger(ProxyProducer.class);

    private final ProxyProducerConfig config;
    private ProxyClientFacade facade; // 延迟初始化（start时创建）

    private volatile boolean started = false;

    /**
     * 简化构造函数 - 推荐使用方式（类似RocketMQ）
     *
     * @param producerGroup 生产者组名
     */
    public ProxyProducer(String producerGroup) {
        this.config = new ProxyProducerConfig();
        this.config.setProducerGroup(producerGroup);
        this.facade = null; // 延迟初始化
    }

    /**
     * 完整配置构造函数 - 高级配置场景
     *
     * @param config 生产者配置
     */
    public ProxyProducer(ProxyProducerConfig config) {
        this.config = config;
        this.facade = null; // 延迟初始化
    }

    // ========== 链式配置方法（推荐使用） ==========

    public ProxyProducer setProxyAddrs(String proxyAddrs) {
        this.config.setProxyAddrs(proxyAddrs);
        return this;
    }

    public ProxyProducer setRetryTimes(int retryTimes) {
        this.config.setRetryTimes(retryTimes);
        return this;
    }

    public ProxyProducer setRequestTimeoutMillis(int timeoutMillis) {
        this.config.setRequestTimeoutMillis(timeoutMillis);
        return this;
    }

    public ProxyProducer setEnableMetrics(boolean enableMetrics) {
        this.config.setEnableMetrics(enableMetrics);
        return this;
    }

    public ProxyProducer setEnableTrace(boolean enableTrace) {
        this.config.setEnableTrace(enableTrace);
        return this;
    }

    public ProxyProducer setTlsEnabled(boolean tlsEnabled) {
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
            started = true;
            log.info("ProxyProducer started, group={}, proxyAddrs={}", 
                config.getProducerGroup(), config.getProxyAddrs());
        }
    }

    public void shutdown() {
        if (started) {
            if (facade != null) {
                facade.shutdown();
            }
            started = false;
            log.info("ProxyProducer shutdown, group={}", config.getProducerGroup());
        }
    }

    // ========== 发送消息 ==========

    public SendResult send(String topic, String tags, byte[] body) throws ProxyException {
        return send(topic, tags, null, body, 0);
    }

    public SendResult send(String topic, String tags, String keys, byte[] body) throws ProxyException {
        return send(topic, tags, keys, body, 0);
    }

    public SendResult send(String topic, String tags, String keys, byte[] body, int delayLevel)
            throws ProxyException {
        return send(topic, tags, keys, body, delayLevel, -1);
    }

    public SendResult send(String topic, String tags, String keys, byte[] body, int delayLevel, int queueId)
            throws ProxyException {

        ensureStarted();

        String traceId = null;
        if (config.isEnableTrace()) {
            traceId = facade.getTraceCollector().generateTraceId();
        }

        long startTime = System.currentTimeMillis();

        try {
            RemotingCommand request = buildSendMessageRequest(topic, tags, keys, body, delayLevel, queueId, traceId);
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

    public void sendAsync(String topic, String tags, byte[] body, SendCallback callback) throws ProxyException {
        sendAsync(topic, tags, null, body, 0, callback);
    }

    public void sendAsync(String topic, String tags, String keys, byte[] body,
                          int delayLevel, SendCallback callback) throws ProxyException {

        ensureStarted();

        String traceId = null;
        if (config.isEnableTrace()) {
            traceId = facade.getTraceCollector().generateTraceId();
        }

        long startTime = System.currentTimeMillis();
        final String finalTraceId = traceId;

        try {
            RemotingCommand request = buildSendMessageRequest(topic, tags, keys, body, delayLevel, -1, traceId);

            facade.invokeAsync(request, config.getRequestTimeoutMillis(), new InvokeCallback() {
                @Override
                public void onSuccess(RemotingCommand response) {
                    SendResult result = parseSendResult(response);
                    result.setTraceId(finalTraceId);

                    if (config.isEnableTrace()) {
                        facade.getTraceCollector().recordSendTrace(finalTraceId, topic, result.getMsgId(),
                            startTime, result.isSuccess(), result.isSuccess() ? null : result.getErrorMsg());
                    }
                    callback.onSuccess(result);
                }

                @Override
                public void onException(Throwable cause) {
                    if (config.isEnableTrace() && finalTraceId != null) {
                        facade.getTraceCollector().recordSendTrace(finalTraceId, topic, null,
                            startTime, false, cause.getMessage());
                    }
                    callback.onException(cause);
                }
            });

        } catch (Exception e) {
            if (config.isEnableTrace() && traceId != null) {
                facade.getTraceCollector().recordSendTrace(traceId, topic, null,
                    startTime, false, e.getMessage());
            }
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Send async message failed", e);
        }
    }

    public void sendOneway(String topic, String tags, byte[] body) throws ProxyException {
        sendOneway(topic, tags, null, body, 0);
    }

    public void sendOneway(String topic, String tags, String keys, byte[] body, int delayLevel)
            throws ProxyException {

        ensureStarted();

        try {
            RemotingCommand request = buildSendMessageRequest(topic, tags, keys, body, delayLevel, -1, null);
            facade.invokeOneway(request);
        } catch (Exception e) {
            if (e instanceof ProxyException) {
                throw e;
            }
            throw new ProxyException("Send oneway message failed", e);
        }
    }

    // ========== 监控 ==========

    public Map<String, ProxyMetricsSnapshot> getMetrics() {
        ensureStarted();
        return facade.getMetricsCollector().getSnapshot();
    }

    public ProxyProducerConfig getConfig() {
        return config;
    }

    // ========== 私有方法 ==========

    private void ensureStarted() {
        if (!started) {
            throw new IllegalStateException("ProxyProducer not started, call start() first");
        }
        if (facade == null) {
            throw new IllegalStateException("ProxyProducer facade not initialized");
        }
    }

    private RemotingCommand buildSendMessageRequest(String topic, String tags, String keys,
                                                    byte[] body, int delayLevel, int queueId, String traceId) {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(config.getProducerGroup());
        header.setTopic(topic);
        header.setDefaultTopic("TBW102");
        header.setDefaultTopicQueueNums(4);
        header.setQueueId(queueId);
        header.setSysFlag(0);
        header.setBornTimestamp(System.currentTimeMillis());
        header.setFlag(0);

        String properties = buildMessageProperties(tags, keys, traceId, delayLevel);
        header.setProperties(properties);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
        request.setBody(body);
        request.makeCustomHeaderToNet();

        return request;
    }

    private String buildMessageProperties(String tags, String keys, String traceId, int delayLevel) {
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
        if (delayLevel > 0) {
            if (sb.length() > 0) {
                sb.append((char) 2);
            }
            sb.append("DELAY").append((char) 1).append(delayLevel);
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
}
