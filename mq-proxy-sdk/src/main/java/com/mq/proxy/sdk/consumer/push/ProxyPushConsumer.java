package com.mq.proxy.sdk.consumer.push;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyPushConsumer {

    private static final Logger log = LoggerFactory.getLogger(ProxyPushConsumer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ProxyPushConsumerConfig config;
    private final ProxyConsumer pullConsumer;

    private final Map<String, String> subscriptions = new ConcurrentHashMap<>();

    private final Set<MessageQueue> assignedQueues = Collections.synchronizedSet(new HashSet<>());

    private final Map<MessageQueue, Long> offsetTable = new ConcurrentHashMap<>();

    private final Map<MessageQueue, PullTask> pullTasks = new ConcurrentHashMap<>();

    private ProxyMessageListener messageListener;

    private ScheduledExecutorService rebalanceExecutor;
    private ScheduledExecutorService pullExecutor;
    private ExecutorService consumeExecutor;
    private ScheduledExecutorService autoCommitExecutor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private String clientId;
    private volatile String lastRebalanceThreadName;

    public ProxyPushConsumer(String consumerGroup) {
        this.config = new ProxyPushConsumerConfig();
        this.config.setConsumerGroup(consumerGroup);
        this.pullConsumer = new ProxyConsumer(consumerGroup);
    }

    public ProxyPushConsumer(ProxyPushConsumerConfig config) {
        this.config = config;
        this.pullConsumer = new ProxyConsumer(config.getConsumerGroup());
    }

    public ProxyPushConsumer setProxyAddrs(String proxyAddrs) {
        this.config.setProxyAddrs(proxyAddrs);
        this.pullConsumer.setProxyAddrs(proxyAddrs);
        return this;
    }

    public ProxyPushConsumer setSuspendTimeoutMillis(long suspendTimeoutMillis) {
        this.config.setSuspendTimeoutMillis(suspendTimeoutMillis);
        this.pullConsumer.setSuspendTimeoutMillis(suspendTimeoutMillis);
        return this;
    }

    public ProxyPushConsumer setMessageModel(String messageModel) {
        this.config.setMessageModel(messageModel);
        this.pullConsumer.setMessageModel(messageModel);
        return this;
    }

    public ProxyPushConsumer setRequestTimeoutMillis(int timeoutMillis) {
        this.config.setRequestTimeoutMillis(timeoutMillis);
        this.pullConsumer.setRequestTimeoutMillis(timeoutMillis);
        return this;
    }

    public ProxyPushConsumer setRetryTimes(int retryTimes) {
        this.config.setRetryTimes(retryTimes);
        this.pullConsumer.setRetryTimes(retryTimes);
        return this;
    }

    public ProxyPushConsumerConfig getConfig() {
        return config;
    }

    public void subscribe(String topic, String subExpression) {
        subscriptions.put(topic, subExpression);
        pullConsumer.subscribe(topic, subExpression);
    }

    public void registerMessageListener(ProxyMessageListener listener) {
        this.messageListener = listener;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        if (subscriptions.isEmpty()) {
            throw new IllegalStateException("No topic subscribed");
        }
        if (messageListener == null) {
            throw new IllegalStateException("No message listener registered");
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
                Thread t = new Thread(r, "RebalanceThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
        pullExecutor = new ScheduledThreadPoolExecutor(config.getWorkerThreadNums(), new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "PullThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
        consumeExecutor = new ThreadPoolExecutor(config.getConsumeThreadNums(), config.getConsumeThreadNums(),
            60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(100000), new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "ConsumeMessageThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
        autoCommitExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "AutoCommitThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });

        registerRebalanceProcessor();

        pullConsumer.start();

        this.clientId = pullConsumer.getClientId();

        doRebalance();
        rebalanceExecutor.scheduleWithFixedDelay(this::doRebalance,
            config.getRebalanceIntervalMillis(),
            config.getRebalanceIntervalMillis(),
            TimeUnit.MILLISECONDS);

        if (config.getAutoCommitIntervalMillis() > 0) {
            autoCommitExecutor.scheduleWithFixedDelay(this::commitAllOffsets,
                config.getAutoCommitIntervalMillis(),
                config.getAutoCommitIntervalMillis(),
                TimeUnit.MILLISECONDS);
        }

        log.info("ProxyPushConsumer started, group={}, proxyAddrs={}", config.getConsumerGroup(), config.getProxyAddrs());
    }

    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        cancelAllPullTasks();

        try {
            commitAllOffsets();
        } catch (Exception e) {
            log.warn("Failed to commit offsets during shutdown", e);
        }

        shutdownExecutor(rebalanceExecutor);
        shutdownExecutor(pullExecutor);
        shutdownExecutor(consumeExecutor);
        shutdownExecutor(autoCommitExecutor);

        pullConsumer.shutdown();

        log.info("ProxyPushConsumer shutdown, group={}", config.getConsumerGroup());
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

    private void registerRebalanceProcessor() {
        pullConsumer.registerConsumerChangeListener(new ConsumerChangeListener() {
            @Override
            public void onConsumerIdsChanged(String consumerGroup) {
                if (consumerGroup != null && consumerGroup.equals(config.getConsumerGroup())) {
                    log.info("Consumer ids changed for group {}, triggering rebalance", consumerGroup);
                    triggerRebalance();
                }
            }
        });
    }

    private void triggerRebalance() {
        ScheduledExecutorService executor = rebalanceExecutor;
        if (executor != null) {
            executor.execute(this::doRebalance);
            return;
        }
        doRebalance();
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
        long initialOffset = -1L;
        try {
            initialOffset = pullConsumer.queryConsumerOffset(config.getConsumerGroup(), mq.getTopic(), mq.getQueueId());
            if (initialOffset < 0) {
                initialOffset = 0L;
            }
        } catch (Exception e) {
            log.warn("Failed to query offset for {}, starting from 0", mq);
            initialOffset = 0L;
        }
        offsetTable.put(mq, initialOffset);
        PullTask task = new PullTask(mq);
        pullTasks.put(mq, task);
        pullExecutor.schedule(task, 0, TimeUnit.MILLISECONDS);
    }

    private void cancelPullTask(MessageQueue mq) {
        PullTask task = pullTasks.remove(mq);
        if (task != null) {
            task.cancel();
        }
        offsetTable.remove(mq);
    }

    private void cancelAllPullTasks() {
        for (PullTask task : pullTasks.values()) {
            task.cancel();
        }
        pullTasks.clear();
    }

    private void commitAllOffsets() {
        for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            Long offset = entry.getValue();
            if (offset != null && offset > 0) {
                try {
                    pullConsumer.updateConsumerOffset(config.getConsumerGroup(), mq.getTopic(), mq.getQueueId(), offset);
                } catch (Exception e) {
                    log.warn("Failed to commit offset for {}: {}", mq, e.getMessage());
                }
            }
        }
    }

    private void dispatchToListener(List<ProxyMessage> messages) {
        consumeExecutor.execute(() -> {
            try {
                if (messageListener != null) {
                    messageListener.consume(messages);
                }
            } catch (Exception e) {
                log.error("Message listener error", e);
            }
        });
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

            long offset = offsetTable.getOrDefault(mq, 0L);
            long commitOffset = offset;

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
                        if (!decodedMessages.isEmpty()) {
                            List<ProxyMessage> proxyMessages = new ArrayList<>();
                            for (DecodedMessage dm : decodedMessages) {
                                ProxyMessage message = new ProxyMessage(
                                    dm.getTopic() != null ? dm.getTopic() : mq.getTopic(),
                                    dm.getQueueId(),
                                    dm.getQueueOffset(),
                                    dm.getBody()
                                );
                                proxyMessages.add(message);
                            }
                            dispatchToListener(proxyMessages);
                        }
                    }
                    offsetTable.put(mq, result.getNextBeginOffset());
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
                processQueueInfo.setCommitOffset(offsetTable.getOrDefault(sdkMq, 0L));
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
