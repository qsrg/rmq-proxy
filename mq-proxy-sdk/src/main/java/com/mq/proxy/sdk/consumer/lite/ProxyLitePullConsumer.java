package com.mq.proxy.sdk.consumer.lite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import com.mq.proxy.sdk.consumer.ConsumerChangeListener;
import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.PullResult;
import com.mq.proxy.sdk.consumer.model.DecodedMessage;
import com.mq.proxy.sdk.consumer.model.MessageQueue;
import com.mq.proxy.sdk.consumer.model.ProxyMessage;
import com.mq.proxy.sdk.exception.ProxyException;

import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyLitePullConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProxyLitePullConsumer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final long PULL_TIME_DELAY_MILLS_WHEN_CACHE_FULL = 100;

    private final ProxyLitePullConsumerConfig config;
    private final ProxyConsumer pullConsumer;

    private final Map<String, String> subscriptions = new ConcurrentHashMap<>();

    private final Set<MessageQueue> assignedQueues = Collections.synchronizedSet(new HashSet<>());

    private final Map<MessageQueue, Long> pullOffsetTable = new ConcurrentHashMap<>();

    private final Map<MessageQueue, Long> consumeOffsetTable = new ConcurrentHashMap<>();

    private final Map<MessageQueue, Long> seekOffsetTable = new ConcurrentHashMap<>();

    private final Map<MessageQueue, PullTask> pullTasks = new ConcurrentHashMap<>();

    private final Map<MessageQueue, BlockingQueue<ProxyMessage>> messageCache = new ConcurrentHashMap<>();

    private final BlockingQueue<ProxyMessage> consumeRequestQueue = new LinkedBlockingQueue<>();

    private ScheduledExecutorService rebalanceExecutor;
    private ScheduledExecutorService pullExecutor;
    private ScheduledExecutorService autoCommitExecutor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private String clientId;
    private volatile String lastRebalanceThreadName;

    private long nextAutoCommitDeadline = -1L;

    public ProxyLitePullConsumer(String consumerGroup) {
        this.config = new ProxyLitePullConsumerConfig();
        this.config.setConsumerGroup(consumerGroup);
        this.pullConsumer = new ProxyConsumer(consumerGroup);
    }

    public ProxyLitePullConsumer(ProxyLitePullConsumerConfig config) {
        this.config = config;
        this.pullConsumer = new ProxyConsumer(config.getConsumerGroup());
    }

    public ProxyLitePullConsumer setProxyAddrs(String proxyAddrs) {
        this.config.setProxyAddrs(proxyAddrs);
        this.pullConsumer.setProxyAddrs(proxyAddrs);
        return this;
    }

    public ProxyLitePullConsumer setSuspendTimeoutMillis(long suspendTimeoutMillis) {
        this.config.setSuspendTimeoutMillis(suspendTimeoutMillis);
        this.pullConsumer.setSuspendTimeoutMillis(suspendTimeoutMillis);
        return this;
    }

    public ProxyLitePullConsumer setMessageModel(String messageModel) {
        this.config.setMessageModel(messageModel);
        this.pullConsumer.setMessageModel(messageModel);
        return this;
    }

    public ProxyLitePullConsumer setRequestTimeoutMillis(int timeoutMillis) {
        this.config.setRequestTimeoutMillis(timeoutMillis);
        this.pullConsumer.setRequestTimeoutMillis(timeoutMillis);
        return this;
    }

    public ProxyLitePullConsumer setRetryTimes(int retryTimes) {
        this.config.setRetryTimes(retryTimes);
        this.pullConsumer.setRetryTimes(retryTimes);
        return this;
    }

    public ProxyLitePullConsumer setPullBatchSize(int pullBatchSize) {
        this.config.setPullBatchSize(pullBatchSize);
        return this;
    }

    public ProxyLitePullConsumer setAutoCommit(boolean autoCommit) {
        this.config.setAutoCommit(autoCommit);
        return this;
    }

    public ProxyLitePullConsumerConfig getConfig() {
        return config;
    }

    public void subscribe(String topic, String subExpression) {
        subscriptions.put(topic, subExpression);
        pullConsumer.subscribe(topic, subExpression);
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        if (subscriptions.isEmpty()) {
            throw new IllegalStateException("No topic subscribed");
        }

        pullConsumer.setProxyAddrs(config.getProxyAddrs())
            .setSuspendTimeoutMillis(config.getSuspendTimeoutMillis())
            .setMessageModel(config.getMessageModel())
            .setRequestTimeoutMillis(config.getRequestTimeoutMillis())
            .setRetryTimes(config.getRetryTimes());
        pullConsumer.getConfig().setConsumeType("CONSUME_PASSIVELY");
        pullConsumer.setConsumerRunningInfoProvider(this::snapshotProcessQueueTable);

        rebalanceExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "LiteRebalanceThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
        pullExecutor = new ScheduledThreadPoolExecutor(config.getWorkerThreadNums(), new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "LitePullThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });

        pullConsumer.registerConsumerChangeListener(new ConsumerChangeListener() {
            @Override
            public void onConsumerIdsChanged(String consumerGroup) {
                if (consumerGroup != null && consumerGroup.equals(config.getConsumerGroup())) {
                    log.info("Consumer ids changed for group {}, triggering rebalance", consumerGroup);
                    triggerRebalance();
                }
            }
        });

        pullConsumer.start();

        this.clientId = pullConsumer.getClientId();

        doRebalance();
        rebalanceExecutor.scheduleWithFixedDelay(this::doRebalance,
            config.getRebalanceIntervalMillis(),
            config.getRebalanceIntervalMillis(),
            TimeUnit.MILLISECONDS);

        if (config.isAutoCommit()) {
            this.nextAutoCommitDeadline = System.currentTimeMillis() + config.getAutoCommitIntervalMillis();
        }

        log.info("ProxyLitePullConsumer started, group={}, proxyAddrs={}", config.getConsumerGroup(), config.getProxyAddrs());
    }

    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        cancelAllPullTasks();

        try {
            commitAll();
        } catch (Exception e) {
            log.warn("Failed to commit offsets during shutdown", e);
        }

        shutdownExecutor(rebalanceExecutor);
        shutdownExecutor(pullExecutor);
        shutdownExecutor(autoCommitExecutor);

        pullConsumer.shutdown();

        log.info("ProxyLitePullConsumer shutdown, group={}", config.getConsumerGroup());
    }

    private void triggerRebalance() {
        ScheduledExecutorService executor = rebalanceExecutor;
        if (executor != null) {
            executor.execute(this::doRebalance);
            return;
        }
        doRebalance();
    }

    public List<ProxyMessage> poll(long timeoutMillis) {
        checkStarted();

        if (config.isAutoCommit()) {
            maybeAutoCommit();
        }

        try {
            List<ProxyMessage> result = new ArrayList<>();
            ProxyMessage first = consumeRequestQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (first != null) {
                result.add(first);
                consumeRequestQueue.drainTo(result, config.getPullBatchSize() - 1);
                for (ProxyMessage msg : result) {
                    MessageQueue assignedMq = findAssignedQueue(msg.getTopic(), msg.getQueueId());
                    if (assignedMq != null) {
                        consumeOffsetTable.put(assignedMq, msg.getQueueOffset() + 1);
                    }
                }
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Collections.emptyList();
        }
    }

    public List<ProxyMessage> poll() {
        return poll(Long.MAX_VALUE);
    }

    @SuppressWarnings("deprecation")
    public void seek(String topic, int queueId, long offset) {
        checkStarted();

        MessageQueue mq = findAssignedQueue(topic, queueId);
        if (mq == null) {
            throw new IllegalArgumentException("Queue not assigned: topic=" + topic + ", queueId=" + queueId);
        }

        seekOffsetTable.put(mq, offset);

        PullTask oldTask = pullTasks.remove(mq);
        if (oldTask != null) {
            oldTask.cancel();
        }

        consumeRequestQueue.removeIf(msg -> msg.getQueueId() == queueId && topic.equals(msg.getTopic()));

        PullTask newTask = new PullTask(mq);
        pullTasks.put(mq, newTask);
        pullExecutor.schedule(newTask, 0, TimeUnit.MILLISECONDS);
    }

    public void seekToBegin(String topic, int queueId) {
        seek(topic, queueId, 0L);
    }

    public void commitAll() {
        for (MessageQueue mq : assignedQueues) {
            Long offset = consumeOffsetTable.get(mq);
            if (offset != null && offset > 0) {
                try {
                    pullConsumer.updateConsumerOffset(config.getConsumerGroup(), mq.getTopic(), mq.getQueueId(), offset);
                } catch (Exception e) {
                    log.warn("Failed to commit offset for {}: {}", mq, e.getMessage());
                }
            }
        }
    }

    public Set<MessageQueue> assignment() {
        return Collections.unmodifiableSet(new HashSet<>(assignedQueues));
    }

    private void checkStarted() {
        if (!started.get()) {
            throw new IllegalStateException("Consumer not started");
        }
    }

    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        if (now >= nextAutoCommitDeadline) {
            commitAll();
            nextAutoCommitDeadline = now + config.getAutoCommitIntervalMillis();
        }
    }

    private MessageQueue findAssignedQueue(String topic, int queueId) {
        for (MessageQueue mq : assignedQueues) {
            if (mq.getTopic().equals(topic) && mq.getQueueId() == queueId) {
                return mq;
            }
        }
        return null;
    }

    private void doRebalance() {
        lastRebalanceThreadName = Thread.currentThread().getName();
        try {
            List<MessageQueue> allQueues = fetchTopicQueues();
            if (allQueues.isEmpty()) {
                return;
            }

            List<String> consumerIds = fetchConsumerIds();
            if (consumerIds.isEmpty()) {
                return;
            }

            int myIndex = consumerIds.indexOf(this.clientId);
            if (myIndex < 0) {
                myIndex = consumerIds.size();
                consumerIds = new ArrayList<>(consumerIds);
                consumerIds.add(this.clientId);
            }

            int consumerCount = consumerIds.size();
            Set<MessageQueue> newAssigned = new HashSet<>();

            for (int i = 0; i < allQueues.size(); i++) {
                if (i % consumerCount == myIndex) {
                    newAssigned.add(allQueues.get(i));
                }
            }

            Set<MessageQueue> prevAssigned = new HashSet<>(assignedQueues);

            for (MessageQueue mq : prevAssigned) {
                if (!newAssigned.contains(mq)) {
                    cancelPullTask(mq);
                    assignedQueues.remove(mq);
                    log.info("Queue removed from assignment: {}", mq);
                }
            }

            for (MessageQueue mq : newAssigned) {
                if (!assignedQueues.contains(mq)) {
                    assignedQueues.add(mq);
                    startPullTask(mq);
                    log.info("Queue assigned: {}", mq);
                }
            }

        } catch (Exception e) {
            log.warn("Rebalance error", e);
        }
    }

    private List<MessageQueue> fetchTopicQueues() throws ProxyException {
        List<MessageQueue> result = new ArrayList<>();
        for (String topic : subscriptions.keySet()) {
            GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader(topic);
            RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, header);
            request.makeCustomHeaderToNet();

            RemotingCommand response = pullConsumer.getFacade().invokeSync(request, config.getRequestTimeoutMillis());
            if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
                byte[] body = response.getBody();
                if (body != null && body.length > 0) {
                    TopicRouteInfo routeInfo = RouteInfoSerializer.decodeTopicRouteInfo(body);
                    if (routeInfo.getQueueDatas() != null) {
                        for (TopicRouteInfo.QueueData qd : routeInfo.getQueueDatas()) {
                            if (qd.getPerm() == 6 && qd.getReadQueueNums() > 0) {
                                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                                    result.add(new MessageQueue(topic, qd.getBrokerName(), i));
                                }
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private List<String> fetchConsumerIds() throws ProxyException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", config.getConsumerGroup());
        request.setExtFields(extFields);

        RemotingCommand response = pullConsumer.getFacade().invokeSync(request, config.getRequestTimeoutMillis());
        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null && body.length > 0) {
                try {
                    Map<String, Object> json = MAPPER.readValue(body, Map.class);
                    List<String> idList = (List<String>) json.get("consumerIdList");
                    if (idList != null) {
                        Collections.sort(idList);
                        return idList;
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse consumer list response", e);
                }
            }
        }
        return Collections.singletonList(this.clientId);
    }

    private void startPullTask(MessageQueue mq) {
        long offset = -1L;
        Long seekOffset = seekOffsetTable.get(mq);
        if (seekOffset != null) {
            offset = seekOffset;
            seekOffsetTable.remove(mq);
        } else {
            try {
                offset = pullConsumer.queryConsumerOffset(config.getConsumerGroup(), mq.getTopic(), mq.getQueueId());
                if (offset < 0) {
                    offset = 0L;
                }
            } catch (Exception e) {
                log.warn("Failed to query offset for {}, starting from 0", mq);
                offset = 0L;
            }
        }
        pullOffsetTable.put(mq, offset);
        consumeOffsetTable.put(mq, offset);
        messageCache.put(mq, new LinkedBlockingQueue<>());

        PullTask task = new PullTask(mq);
        pullTasks.put(mq, task);
        pullExecutor.schedule(task, 0, TimeUnit.MILLISECONDS);
    }

    private void cancelPullTask(MessageQueue mq) {
        PullTask task = pullTasks.remove(mq);
        if (task != null) {
            task.cancel();
        }
        pullOffsetTable.remove(mq);
        consumeOffsetTable.remove(mq);
        messageCache.remove(mq);
    }

    private void cancelAllPullTasks() {
        for (PullTask task : pullTasks.values()) {
            task.cancel();
        }
        pullTasks.clear();
    }

    private void submitConsumeRequest(ProxyMessage message) {
        try {
            consumeRequestQueue.put(message);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Submit consume request interrupted");
        }
    }

    private class PullTask implements Runnable {

        private final MessageQueue mq;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);

        PullTask(MessageQueue mq) {
            this.mq = mq;
        }

        void cancel() {
            cancelled.set(true);
        }

        @Override
        public void run() {
            if (cancelled.get()) {
                return;
            }

            if (!assignedQueues.contains(mq)) {
                return;
            }

            if (seekOffsetTable.containsKey(mq)) {
                pullExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FULL, TimeUnit.MILLISECONDS);
                return;
            }

            BlockingQueue<ProxyMessage> cache = messageCache.get(mq);
            if (cache != null && cache.size() > config.getPullThresholdForQueue()) {
                pullExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_CACHE_FULL, TimeUnit.MILLISECONDS);
                return;
            }

            long offset = pullOffsetTable.getOrDefault(mq, 0L);
            long commitOffset = consumeOffsetTable.getOrDefault(mq, offset);

            try {
                PullResult result = pullConsumer.pull(
                    mq.getTopic(),
                    config.getConsumerGroup(),
                    mq.getQueueId(),
                    offset,
                    config.getPullBatchSize(),
                    commitOffset
                );

                if (result != null && result.isFound()) {
                    byte[] body = result.getBody();
                    if (body != null && body.length > 0) {
                        List<DecodedMessage> decodedMessages = DecodedMessage.decode(body);
                        for (DecodedMessage dm : decodedMessages) {
                            ProxyMessage message = new ProxyMessage(
                                dm.getTopic() != null ? dm.getTopic() : mq.getTopic(),
                                dm.getQueueId(),
                                dm.getQueueOffset(),
                                dm.getBody()
                            );
                            submitConsumeRequest(message);
                        }
                    }
                    pullOffsetTable.put(mq, result.getNextBeginOffset());
                }

            } catch (Exception e) {
                log.warn("Pull error for {}: {}", mq, e.getMessage());
            }

            if (!cancelled.get()) {
                long interval = config.getPullIntervalMillis();
                if (interval > 0) {
                    pullExecutor.schedule(this, interval, TimeUnit.MILLISECONDS);
                } else {
                    pullExecutor.schedule(this, 0, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void shutdownExecutor(java.util.concurrent.ExecutorService executor) {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private TreeMap<org.apache.rocketmq.common.message.MessageQueue, ProcessQueueInfo> snapshotProcessQueueTable() {
        TreeMap<org.apache.rocketmq.common.message.MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<>();
        long now = System.currentTimeMillis();
        synchronized (assignedQueues) {
            for (MessageQueue sdkMq : assignedQueues) {
                org.apache.rocketmq.common.message.MessageQueue mq =
                    new org.apache.rocketmq.common.message.MessageQueue(
                        sdkMq.getTopic(), sdkMq.getBrokerName(), sdkMq.getQueueId());
                ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
                processQueueInfo.setCommitOffset(consumeOffsetTable.getOrDefault(sdkMq,
                    pullOffsetTable.getOrDefault(sdkMq, 0L)));
                processQueueInfo.setLocked(true);
                processQueueInfo.setLastLockTimestamp(now);
                processQueueInfo.setLastPullTimestamp(now);
                processQueueInfo.setLastConsumeTimestamp(now);
                mqTable.put(mq, processQueueInfo);
            }
        }
        return mqTable;
    }
}
