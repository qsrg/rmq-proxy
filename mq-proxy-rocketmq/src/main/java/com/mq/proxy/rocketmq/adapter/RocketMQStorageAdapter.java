package com.mq.proxy.rocketmq.adapter;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageRequestHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeaderV2;
import com.mq.proxy.core.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mq.proxy.core.server.InvokeCallback;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyClientRuntime;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.PullMessageCallback;
import com.mq.proxy.core.storage.PutMessageCallback;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RocketMQStorageAdapter implements StorageAdapter {

    private static final Logger log = LoggerFactory.getLogger(RocketMQStorageAdapter.class);
    private static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 3000L;
    private static final long LONG_POLL_TIMEOUT_MARGIN_MILLIS = 5000L;
    private static final long MIN_LONG_POLL_REQUEST_TIMEOUT_MILLIS = 30000L;
    private static final long UPSTREAM_STATS_LOG_INTERVAL_SECONDS = 10L;

    private NettyRemotingClient remotingClient;
    private NettyRemotingClient[] remotingClients;
    private NettyRemotingClient[] producerRemotingClients;
    private NettyRemotingClient[] pullRemotingClients;
    private NettyClientRuntime remotingClientRuntime;
    private ScheduledExecutorService upstreamStatsLogExecutor;
    private final AtomicInteger remotingClientSelector = new AtomicInteger(0);
    private final AtomicInteger producerRemotingClientSelector = new AtomicInteger(0);
    private final AtomicInteger pullRemotingClientSelector = new AtomicInteger(0);
    private StorageConfig storageConfig;
    private volatile boolean initialized = false;

    private static final int FLAG_COMMIT_OFFSET = 0x1 << 0;
    private static final int FLAG_SUSPEND = 0x1 << 1;
    private static final int FLAG_SUBSCRIPTION = 0x1 << 2;

    @Override
    public void initialize(StorageConfig config) throws Exception {
        this.storageConfig = config;
        int poolSize = Math.max(1, config.getUpstreamClientChannelPoolSize());
        NettyClientConfig producerClientConfig = createNettyClientConfig(config,
                config.getUpstreamProducerAsyncSemaphoreValue(), poolSize);
        NettyClientConfig pullClientConfig = createNettyClientConfig(config,
                config.getUpstreamPullAsyncSemaphoreValue(), poolSize);

        this.remotingClientRuntime = new NettyClientRuntime(producerClientConfig, "RocketMQStorageClient");
        this.producerRemotingClients = new NettyRemotingClient[poolSize];
        this.pullRemotingClients = new NettyRemotingClient[poolSize];
        for (int i = 0; i < poolSize; i++) {
            this.producerRemotingClients[i] = new NettyRemotingClient(producerClientConfig, this.remotingClientRuntime);
            this.producerRemotingClients[i].start();
            this.pullRemotingClients[i] = new NettyRemotingClient(pullClientConfig, this.remotingClientRuntime);
            this.pullRemotingClients[i].start();
        }
        this.remotingClients = this.producerRemotingClients;
        this.remotingClient = this.producerRemotingClients[0];
        startUpstreamStatsLog();
        this.initialized = true;
        log.info("RocketMQStorageAdapter initialized: upstreamClientChannelPoolSize={}, upstreamClientAsyncSemaphoreValue={}, upstreamProducerAsyncSemaphoreValue={}, upstreamPullAsyncSemaphoreValue={}, upstreamClientKeepAliveIntervalSeconds={}, upstreamTlsEnabled={}",
                poolSize, config.getUpstreamClientAsyncSemaphoreValue(),
                config.getUpstreamProducerAsyncSemaphoreValue(), config.getUpstreamPullAsyncSemaphoreValue(),
                config.getUpstreamClientKeepAliveIntervalSeconds(), config.isUpstreamTlsEnabled());
    }

    @Override
    public void shutdown() {
        if (this.upstreamStatsLogExecutor != null) {
            this.upstreamStatsLogExecutor.shutdown();
            this.upstreamStatsLogExecutor = null;
        }
        if (this.producerRemotingClients != null || this.pullRemotingClients != null) {
            shutdownClients(this.producerRemotingClients);
            shutdownClients(this.pullRemotingClients);
            this.producerRemotingClients = null;
            this.pullRemotingClients = null;
            this.remotingClients = null;
        } else if (this.remotingClient != null) {
            this.remotingClient.shutdown();
        }
        if (this.remotingClientRuntime != null) {
            this.remotingClientRuntime.shutdown();
            this.remotingClientRuntime = null;
        }
        this.remotingClient = null;
        this.initialized = false;
    }

    private void startUpstreamStatsLog() {
        this.upstreamStatsLogExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            private final AtomicInteger threadIndex = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "RocketMQStorageStatsLogger_" + threadIndex.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        this.upstreamStatsLogExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    log.info(buildUpstreamClientStatsLog());
                } catch (Throwable throwable) {
                    log.warn("Failed to log upstream client stats: {}", throwable.getMessage());
                }
            }
        }, UPSTREAM_STATS_LOG_INTERVAL_SECONDS, UPSTREAM_STATS_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    String buildUpstreamClientStatsLog() {
        StringBuilder builder = new StringBuilder("RocketMQStorageAdapter upstream client stats: ");
        appendClientStats(builder, "producer", this.producerRemotingClients);
        builder.append(", ");
        appendClientStats(builder, "pull", this.pullRemotingClients);
        return builder.toString();
    }

    private void appendClientStats(StringBuilder builder, String type, NettyRemotingClient[] clients) {
        builder.append(type).append("=");
        if (clients == null || clients.length == 0) {
            builder.append("[]");
            return;
        }
        builder.append("[");
        for (int i = 0; i < clients.length; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            NettyRemotingClient client = clients[i];
            if (client == null) {
                builder.append(type).append("[").append(i).append("]{null}");
                continue;
            }
            builder.append(type).append("[").append(i).append("]{inFlight=")
                    .append(client.getInFlightRequestCount())
                    .append(", availablePermits=")
                    .append(client.getAsyncSemaphoreAvailablePermits())
                    .append(", limit=")
                    .append(client.getAsyncSemaphoreLimit())
                    .append("}");
        }
        builder.append("]");
    }

    private NettyClientConfig createNettyClientConfig(StorageConfig config, int asyncSemaphoreValue, int poolSize) {
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setNamesrvAddr(config.getNamesrvAddr());
        clientConfig.setConnectTimeoutMillis(config.getConnectTimeoutMillis());
        clientConfig.setClientAsyncSemaphoreValue(Math.max(1, (asyncSemaphoreValue + poolSize - 1) / poolSize));
        clientConfig.setClientKeepAliveIntervalSeconds(config.getUpstreamClientKeepAliveIntervalSeconds());
        clientConfig.setClientKeepAliveRequestCode(RequestCode.CHECK_CLIENT_CONFIG);

        // 上游 TLS 配置（proxy -> broker）
        // 语义与 RocketMQ 客户端 useTLS(true) 一致：开启 TLS 加密传输，
        // 默认不验证 broker 证书（与 RocketMQ 默认 tlsClientAuthServer=false 行为一致）。
        if (config.isUpstreamTlsEnabled()) {
            clientConfig.setTlsEnabled(true);
            clientConfig.setTlsClientCertPath(config.getUpstreamTlsClientCertPath());
            clientConfig.setTlsClientKeyPath(config.getUpstreamTlsClientKeyPath());
            log.info("Upstream TLS enabled for broker connections: clientCertPath={}, clientKeyPath={}",
                    config.getUpstreamTlsClientCertPath(),
                    config.getUpstreamTlsClientKeyPath());
        }

        return clientConfig;
    }

    private void shutdownClients(NettyRemotingClient[] clients) {
        if (clients == null) {
            return;
        }
        for (NettyRemotingClient client : clients) {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public PutResult putMessage(InternalMessage message, String brokerAddr) throws Exception {
        checkInitialized();
        RemotingCommand request = createSendMessageRequest(message);
        String targetAddr = resolveBrokerAddr(brokerAddr);
        RemotingCommand response = selectProducerRemotingClient().invokeSync(targetAddr, request, DEFAULT_REQUEST_TIMEOUT_MILLIS);
        return processPutMessageResponse(response);
    }

    @Override
    public void putMessageAsync(InternalMessage message, String brokerAddr, PutMessageCallback callback) {
        final AtomicBoolean completed = new AtomicBoolean(false);
        final PutMessageCallback onceCallback = new PutMessageCallback() {
            @Override
            public void onSuccess(PutResult putResult) {
                if (completed.compareAndSet(false, true)) {
                    callback.onSuccess(putResult);
                }
            }

            @Override
            public void onException(Throwable throwable) {
                if (completed.compareAndSet(false, true)) {
                    callback.onException(throwable);
                }
            }
        };

        try {
            checkInitialized();
            final RemotingCommand request = createSendMessageRequest(message);
            final String targetAddr = resolveBrokerAddr(brokerAddr);
            selectProducerRemotingClient().invokeAsync(targetAddr, request, DEFAULT_REQUEST_TIMEOUT_MILLIS,
                    new InvokeCallback() {
                        @Override
                        public void operationSucceed(RemotingCommand response) {
                            try {
                                onceCallback.onSuccess(processPutMessageResponse(response));
                            } catch (Throwable throwable) {
                                onceCallback.onException(throwable);
                            }
                        }

                        @Override
                        public void operationFail(Throwable throwable) {
                            onceCallback.onException(throwable);
                        }
                    });
        } catch (Throwable throwable) {
            onceCallback.onException(throwable);
        }
    }

    private RemotingCommand createSendMessageRequest(InternalMessage message) {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(message.getProducerGroup());
        header.setTopic(message.getTopic());
        header.setDefaultTopic(message.getDefaultTopic() != null ? message.getDefaultTopic() : "TBW102");
        header.setDefaultTopicQueueNums(message.getDefaultTopicQueueNums() > 0 ? message.getDefaultTopicQueueNums() : 4);
        header.setQueueId(message.getQueueId() != null ? message.getQueueId() : null);
        header.setSysFlag(message.getSysFlag());
        header.setBornTimestamp(message.getBornTimestamp());
        header.setFlag(message.getFlag());
        header.setProperties(message.getProperties());
        header.setReconsumeTimes(message.getReconsumeTimes());
        header.setUnitMode(message.isUnitMode());
        header.setMaxReconsumeTimes(message.getMaxReconsumeTimes());
        header.setBatch(message.isBatch());
        header.setTopicSysFlag(message.getTopicSysFlag());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
        request.setBody(message.getBody());
        request.makeCustomHeaderToNet();
        return request;
    }

    private PutResult processPutMessageResponse(RemotingCommand response) {
        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            String msgId = response.getExtFields() != null ? response.getExtFields().get("msgId") : null;
            int queueId = response.getExtFields() != null && response.getExtFields().get("queueId") != null
                    ? Integer.parseInt(response.getExtFields().get("queueId")) : -1;
            long queueOffset = response.getExtFields() != null && response.getExtFields().get("queueOffset") != null
                    ? Long.parseLong(response.getExtFields().get("queueOffset")) : -1L;
            return PutResult.success(msgId, queueId, queueOffset);
        } else {
            return PutResult.fail(response.getCode(), response.getRemark());
        }
    }

    @Override
    public PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, int sysFlag, long commitOffset, long suspendTimeoutMillis, String subscription, String expressionType, long subVersion, String brokerAddr) throws Exception {
        checkInitialized();
        PullRequestContext requestContext = createPullRequest(consumerGroup, topic, queueId, queueOffset, maxMsgNums,
                sysFlag, commitOffset, suspendTimeoutMillis, subscription, expressionType, subVersion);

        String targetAddr = resolveBrokerAddr(brokerAddr);
        long timeoutMillis = computePullRequestTimeoutMillis(suspendTimeoutMillis);
        log.debug("pullMessage request to broker: targetAddr={}, group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, timeoutMillis={}, subscription={}, expressionType={}, subVersion={}",
                targetAddr, consumerGroup, topic, queueId, queueOffset, maxMsgNums, requestContext.finalSysFlag,
                commitOffset, suspendTimeoutMillis, timeoutMillis, requestContext.subscription, requestContext.expressionType, subVersion);
        RemotingCommand response = selectPullRemotingClient().invokeSync(targetAddr, requestContext.request, timeoutMillis);
        return processPullResponse(targetAddr, consumerGroup, topic, queueId, queueOffset, response);
    }

    @Override
    public void pullMessageAsync(final String consumerGroup, final String topic, final int queueId, final long queueOffset,
                                 final int maxMsgNums, final int sysFlag, final long commitOffset,
                                 final long suspendTimeoutMillis, final String subscription,
                                 final String expressionType, final long subVersion, String brokerAddr,
                                 final PullMessageCallback callback) throws Exception {
        checkInitialized();
        final PullRequestContext requestContext = createPullRequest(consumerGroup, topic, queueId, queueOffset,
                maxMsgNums, sysFlag, commitOffset, suspendTimeoutMillis, subscription, expressionType, subVersion);
        final String targetAddr = resolveBrokerAddr(brokerAddr);
        final long timeoutMillis = computePullRequestTimeoutMillis(suspendTimeoutMillis);

        log.debug("pullMessage request to broker: targetAddr={}, group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, timeoutMillis={}, subscription={}, expressionType={}, subVersion={}",
                targetAddr, consumerGroup, topic, queueId, queueOffset, maxMsgNums, requestContext.finalSysFlag,
                commitOffset, suspendTimeoutMillis, timeoutMillis, requestContext.subscription,
                requestContext.expressionType, subVersion);

        selectPullRemotingClient().invokeAsync(targetAddr, requestContext.request, timeoutMillis, new InvokeCallback() {
            @Override
            public void operationSucceed(RemotingCommand response) {
                try {
                    callback.onSuccess(processPullResponse(targetAddr, consumerGroup, topic, queueId, queueOffset, response));
                } catch (Exception e) {
                    callback.onException(e);
                }
            }

            @Override
            public void operationFail(Throwable throwable) {
                callback.onException(throwable);
            }
        });
    }

    @Override
    public OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId, String brokerAddr) throws Exception {
        checkInitialized();
        QueryConsumerOffsetRequestHeader header = new QueryConsumerOffsetRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, header);
        request.makeCustomHeaderToNet();

        String targetAddr = resolveBrokerAddr(brokerAddr);
        RemotingCommand response = selectProducerRemotingClient().invokeSync(targetAddr, request, 3000);

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            long offset = response.getExtFields() != null && response.getExtFields().get("offset") != null
                    ? Long.parseLong(response.getExtFields().get("offset")) : -1L;
            return OffsetResult.success(offset);
        } else {
            return OffsetResult.fail(response.getCode(), response.getRemark());
        }
    }

    @Override
    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset, String brokerAddr) throws Exception {
        checkInitialized();
        UpdateConsumerOffsetRequestHeader header = new UpdateConsumerOffsetRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setCommitOffset(commitOffset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, header);
        request.makeCustomHeaderToNet();

        String targetAddr = resolveBrokerAddr(brokerAddr);
        RemotingCommand response = selectProducerRemotingClient().invokeSync(targetAddr, request, 3000);

        if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
            throw new RuntimeException("updateConsumerOffset failed, code: " + response.getCode() + ", remark: " + response.getRemark());
        }
    }

    @Override
    public boolean healthCheck() {
        return this.initialized;
    }

    @Override
    public RemotingCommand forwardToBroker(RemotingCommand request, String brokerAddr) throws Exception {
        checkInitialized();
        String targetAddr = resolveBrokerAddr(brokerAddr);

        RemotingCommand forwardRequest = RemotingCommand.createRequestCommand(request.getCode(), request.getCustomHeader());
        forwardRequest.setExtFields(request.getExtFields());
        forwardRequest.setBody(request.getBody());
        forwardRequest.setFlag(request.getFlag());
        forwardRequest.setRemark(request.getRemark());

        return selectProducerRemotingClient().invokeSync(targetAddr, forwardRequest, 3000);
    }

    private NettyRemotingClient selectProducerRemotingClient() {
        if (this.producerRemotingClients != null && this.producerRemotingClients.length > 0) {
            int index = Math.floorMod(this.producerRemotingClientSelector.getAndIncrement(),
                    this.producerRemotingClients.length);
            return this.producerRemotingClients[index];
        }
        return selectRemotingClient();
    }

    private NettyRemotingClient selectPullRemotingClient() {
        if (this.pullRemotingClients != null && this.pullRemotingClients.length > 0) {
            int index = Math.floorMod(this.pullRemotingClientSelector.getAndIncrement(),
                    this.pullRemotingClients.length);
            return this.pullRemotingClients[index];
        }
        return selectRemotingClient();
    }

    private NettyRemotingClient selectRemotingClient() {
        if (this.remotingClients != null && this.remotingClients.length > 0) {
            int index = Math.floorMod(this.remotingClientSelector.getAndIncrement(), this.remotingClients.length);
            return this.remotingClients[index];
        }
        return this.remotingClient;
    }

    private String resolveBrokerAddr(String brokerAddr) {
        if (brokerAddr == null || brokerAddr.isEmpty()) {
            throw new IllegalStateException("brokerAddr is required");
        }
        return brokerAddr;
    }

    private long computePullRequestTimeoutMillis(long suspendTimeoutMillis) {
        if (suspendTimeoutMillis <= 0) {
            return DEFAULT_REQUEST_TIMEOUT_MILLIS;
        }
        return Math.max(MIN_LONG_POLL_REQUEST_TIMEOUT_MILLIS,
                suspendTimeoutMillis + LONG_POLL_TIMEOUT_MARGIN_MILLIS);
    }

    private PullRequestContext createPullRequest(String consumerGroup, String topic, int queueId, long queueOffset,
                                                 int maxMsgNums, int sysFlag, long commitOffset,
                                                 long suspendTimeoutMillis, String subscription,
                                                 String expressionType, long subVersion) {
        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setQueueOffset(queueOffset);
        header.setMaxMsgNums(maxMsgNums);

        String subExpr = subscription != null && !subscription.isEmpty() ? subscription : "*";
        String exprType = expressionType != null && !expressionType.isEmpty() ? expressionType : "TAG";

        int finalSysFlag = sysFlag;
        if (suspendTimeoutMillis > 0) {
            finalSysFlag |= FLAG_SUSPEND;
        }
        finalSysFlag |= FLAG_SUBSCRIPTION;
        header.setSysFlag(finalSysFlag);
        header.setCommitOffset(commitOffset);
        header.setSuspendTimeoutMillis(suspendTimeoutMillis);
        header.setSubscription(subExpr);
        header.setSubVersion(subVersion);
        header.setExpressionType(exprType);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
        request.makeCustomHeaderToNet();
        return new PullRequestContext(request, finalSysFlag, subExpr, exprType);
    }

    private PullResult processPullResponse(String targetAddr, String consumerGroup, String topic, int queueId,
                                           long queueOffset, RemotingCommand response) {
        log.debug("pullMessage response from broker: targetAddr={}, code={}, remark={}, opaque={}, extFields={}, bodySize={}, serializeType={}",
                targetAddr, response.getCode(), response.getRemark(), response.getOpaque(), response.getExtFields(),
                response.getBody() != null ? response.getBody().length : 0,
                response.getSerializeTypeCurrentRPC());

        if (response.getExtFields() == null || response.getExtFields().isEmpty()) {
            log.warn("pullMessage response from broker has EMPTY extFields! code={}, topic={}, queueId={}, queueOffset={}",
                    response.getCode(), topic, queueId, queueOffset);
        }

        long nextBeginOffset = response.getExtFields() != null && response.getExtFields().get("nextBeginOffset") != null
                ? Long.parseLong(response.getExtFields().get("nextBeginOffset")) : -1L;
        long minOffset = response.getExtFields() != null && response.getExtFields().get("minOffset") != null
                ? Long.parseLong(response.getExtFields().get("minOffset")) : -1L;
        long maxOffset = response.getExtFields() != null && response.getExtFields().get("maxOffset") != null
                ? Long.parseLong(response.getExtFields().get("maxOffset")) : -1L;
        String suggestWhichBrokerId = response.getExtFields() != null && response.getExtFields().get("suggestWhichBrokerId") != null
                ? response.getExtFields().get("suggestWhichBrokerId") : null;

        if (nextBeginOffset == -1L) {
            nextBeginOffset = queueOffset;
            log.warn("pullMessage broker response missing nextBeginOffset, using queueOffset as fallback. targetAddr={}, group={}, topic={}, queueId={}, queueOffset={}, responseCode={}",
                    targetAddr, consumerGroup, topic, queueId, queueOffset, response.getCode());
        }
        if (minOffset == -1L) {
            minOffset = 0;
        }
        if (maxOffset == -1L) {
            maxOffset = queueOffset;
        }

        if ((response.getCode() == ResponseCode.PULL_NOT_FOUND || response.getCode() == ResponseCode.PULL_RETRY_IMMEDIATELY)
                && queueOffset > 0 && nextBeginOffset < queueOffset) {
            log.warn("pullMessage broker suggested rewind on not-found response, preserving queueOffset instead. targetAddr={}, group={}, topic={}, queueId={}, requestedOffset={}, brokerNextBeginOffset={}, responseCode={}",
                    targetAddr, consumerGroup, topic, queueId, queueOffset, nextBeginOffset, response.getCode());
            nextBeginOffset = queueOffset;
        }

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            PullResult result = PullResult.found(response.getBody(), nextBeginOffset, minOffset, maxOffset);
            result.setSuggestWhichBrokerId(suggestWhichBrokerId);
            return result;
        }
        if (response.getCode() == ResponseCode.PULL_NOT_FOUND || response.getCode() == ResponseCode.PULL_RETRY_IMMEDIATELY) {
            PullResult result = PullResult.notFound(nextBeginOffset, minOffset, maxOffset);
            result.setSuggestWhichBrokerId(suggestWhichBrokerId);
            return result;
        }

        PullResult result = new PullResult();
        result.setResponseCode(response.getCode());
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(minOffset);
        result.setMaxOffset(maxOffset);
        result.setSuggestWhichBrokerId(suggestWhichBrokerId);
        return result;
    }

    private void checkInitialized() {
        if (!initialized) {
            throw new IllegalStateException("RocketMQStorageAdapter is not initialized");
        }
    }

    private static class PullRequestContext {
        private final RemotingCommand request;
        private final int finalSysFlag;
        private final String subscription;
        private final String expressionType;

        private PullRequestContext(RemotingCommand request, int finalSysFlag, String subscription, String expressionType) {
            this.request = request;
            this.finalSysFlag = finalSysFlag;
            this.subscription = subscription;
            this.expressionType = expressionType;
        }
    }
}
