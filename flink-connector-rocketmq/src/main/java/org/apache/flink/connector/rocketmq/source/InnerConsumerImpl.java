/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.common.config.RocketMQOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.MessageViewExt;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InnerConsumerImpl implements InnerConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(InnerConsumerImpl.class);

    private final Configuration configuration;
    private final DefaultMQAdminExt adminExt;
    private final DefaultLitePullConsumer consumer;
    private final ExecutorService commonExecutorService;

    public InnerConsumerImpl(Configuration configuration) {
        this.configuration = configuration;
        this.commonExecutorService = buildExecutorService(configuration);

        String accessKey = configuration.getString(RocketMQSourceOptions.OPTIONAL_ACCESS_KEY);
        String secretKey = configuration.getString(RocketMQSourceOptions.OPTIONAL_SECRET_KEY);

        boolean enableTrace = configuration.getBoolean(RocketMQSourceOptions.ENABLE_MESSAGE_TRACE);
        String traceTopic = configuration.getString(RocketMQSourceOptions.CUSTOMIZED_TRACE_TOPIC);

        // Note: sync pull thread num may not enough
        if (!StringUtils.isNullOrWhitespaceOnly(accessKey)
                && !StringUtils.isNullOrWhitespaceOnly(secretKey)) {
            AclClientRPCHook aclClientRpcHook =
                    new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
            this.adminExt = new DefaultMQAdminExt(aclClientRpcHook);
            this.consumer = new DefaultLitePullConsumer(aclClientRpcHook);
        } else {
            this.adminExt = new DefaultMQAdminExt();
            this.consumer = new DefaultLitePullConsumer();
        }

        // Enable message trace if configured (P1 fix — previously a ghost parameter)
        this.consumer.setEnableMsgTrace(enableTrace);
        if (traceTopic != null) {
            this.consumer.setCustomizedTraceTopic(traceTopic);
        }

        String groupId = configuration.getString(RocketMQSourceOptions.CONSUMER_GROUP);
        String endPoints = configuration.getString(RocketMQSourceOptions.ENDPOINTS);

        this.consumer.setNamesrvAddr(endPoints);
        this.consumer.setConsumerGroup(groupId);
        this.consumer.setAutoCommit(false);
        this.consumer.setVipChannelEnabled(false);
        this.consumer.setInstanceName(
                String.join(
                        "#",
                        ManagementFactory.getRuntimeMXBean().getName(),
                        groupId,
                        UUID.randomUUID().toString()));

        // Apply pull batch size from config (P1 fix — previously a ghost parameter)
        long pullBatchSize = configuration.getLong(RocketMQSourceOptions.PULL_BATCH_SIZE);
        if (pullBatchSize > 0) {
            this.consumer.setPullBatchSize((int) pullBatchSize);
        }

        // Apply namespace from config (P2 fix)
        String namespace = configuration.getString(RocketMQOptions.NAMESPACE);
        if (namespace != null && !namespace.isEmpty()) {
            this.consumer.setNamespace(namespace);
        }

        // Log auto-commit configuration for observability (P1 fix)
        boolean autoCommit = configuration.getBoolean(RocketMQSourceOptions.AUTO_COMMIT_OFFSET);
        long commitInterval =
                configuration.getLong(RocketMQSourceOptions.AUTO_COMMIT_OFFSET_INTERVAL);
        LOG.info(
                "Consumer offset commit config: autoCommit={}, commitInterval={}ms, checkpointCommit={}",
                autoCommit,
                commitInterval,
                configuration.getBoolean(RocketMQSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT));

        // Apply common client parameters (P2 fix)
        if (configuration.contains(RocketMQOptions.OPTIONAL_ACCESS_CHANNEL)) {
            this.consumer.setAccessChannel(
                    configuration.get(RocketMQOptions.OPTIONAL_ACCESS_CHANNEL));
        }
        boolean unitMode = configuration.getBoolean(RocketMQOptions.OPTIONAL_UNIT_MODE);
        this.consumer.setUnitMode(unitMode);
        String unitName = configuration.getString(RocketMQOptions.OPTIONAL_UNIT_NAME);
        if (unitName != null) {
            this.consumer.setUnitName(unitName);
        }
        if (configuration.contains(RocketMQOptions.CLIENT_CALLBACK_EXECUTOR_THREADS)) {
            this.consumer.setClientCallbackExecutorThreads(
                    configuration.getInteger(RocketMQOptions.CLIENT_CALLBACK_EXECUTOR_THREADS));
        }
        if (configuration.contains(RocketMQOptions.USE_TLS)) {
            this.consumer.setUseTLS(configuration.getBoolean(RocketMQOptions.USE_TLS));
        }
        if (configuration.contains(RocketMQOptions.MQ_CLIENT_API_TIMEOUT)) {
            this.consumer.setMqClientApiTimeout(
                    (int) configuration.getLong(RocketMQOptions.MQ_CLIENT_API_TIMEOUT));
        }
        if (configuration.contains(RocketMQSourceOptions.PULL_THRESHOLD_FOR_QUEUE)) {
            this.consumer.setPullThresholdForQueue(
                    (int) configuration.getLong(RocketMQSourceOptions.PULL_THRESHOLD_FOR_QUEUE));
        }
        if (configuration.contains(RocketMQSourceOptions.PULL_THRESHOLD_FOR_ALL)) {
            this.consumer.setPullThresholdForAll(
                    (int) configuration.getLong(RocketMQSourceOptions.PULL_THRESHOLD_FOR_ALL));
        }
        if (configuration.contains(RocketMQSourceOptions.PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION)) {
            this.consumer.setPullTimeDelayMillsWhenException(
                    configuration.getLong(
                            RocketMQSourceOptions.PULL_TIME_DELAY_MILLS_WHEN_EXCEPTION));
        }
        boolean debugMode = configuration.getBoolean(RocketMQOptions.GLOBAL_DEBUG_MODE);
        if (debugMode) {
            System.setProperty(
                    "rocketmq.client.logRoot",
                    System.getProperty("user.home") + "/logs/rocketmqlogs");
            LOG.info("RocketMQ debug mode enabled");
        }

        this.adminExt.setNamesrvAddr(endPoints);
        this.adminExt.setAdminExtGroup(groupId);
        this.adminExt.setVipChannelEnabled(false);
        if (configuration.contains(RocketMQOptions.OPTIONAL_ACCESS_CHANNEL)) {
            this.adminExt.setAccessChannel(
                    configuration.get(RocketMQOptions.OPTIONAL_ACCESS_CHANNEL));
        }
        if (namespace != null && !namespace.isEmpty()) {
            this.adminExt.setNamespace(namespace);
        }
        if (configuration.contains(RocketMQOptions.USE_TLS)) {
            this.adminExt.setUseTLS(configuration.getBoolean(RocketMQOptions.USE_TLS));
        }
        if (configuration.contains(RocketMQOptions.MQ_CLIENT_API_TIMEOUT)) {
            this.adminExt.setMqClientApiTimeout(
                    (int) configuration.getLong(RocketMQOptions.MQ_CLIENT_API_TIMEOUT));
        }
        this.adminExt.setInstanceName(
                String.join(
                        "#",
                        ManagementFactory.getRuntimeMXBean().getName(),
                        groupId,
                        UUID.randomUUID().toString()));
    }

    @Override
    public void start() {
        try {
            this.adminExt.start();
            this.consumer.start();
            LOG.info(
                    "RocketMQ consumer started success, group={}, consumerId={}",
                    this.consumer.getConsumerGroup(),
                    this.consumer.getInstanceName());
        } catch (Throwable t) {
            LOG.error("RocketMQ consumer started failed", t);
            throw new FlinkRuntimeException("RocketMQ consumer started failed.", t);
        }
    }

    @Override
    public void close() throws Exception {
        this.commonExecutorService.shutdown();
        this.adminExt.shutdown();
        this.consumer.shutdown();
    }

    private ExecutorService buildExecutorService(Configuration configuration) {
        int processors = Runtime.getRuntime().availableProcessors();
        int threadNum =
                configuration.getInteger(RocketMQSourceOptions.PULL_THREADS_NUM, processors);
        return new ThreadPoolExecutor(
                threadNum,
                threadNum,
                TimeUnit.MINUTES.toMillis(1),
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new ThreadFactoryImpl("RocketMQCommonExecutorThread_"));
    }

    @Override
    public String getConsumerGroup() {
        return configuration.getString(RocketMQSourceOptions.CONSUMER_GROUP);
    }

    @Override
    public CompletableFuture<Collection<MessageQueue>> fetchMessageQueues(String topic) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        Collection<MessageQueue> result = consumer.fetchMessageQueues(topic);
                        LOG.info(
                                "Consumer request topic route for service discovery, topic={}, route={}",
                                topic,
                                JSON.toJSONString(result));
                        return result;
                    } catch (Exception e) {
                        LOG.error(
                                "Consumer request topic route for service discovery, topic={}, nsAddress={}",
                                topic,
                                this.consumer.getNamesrvAddr(),
                                e);
                    }
                    return Collections.emptyList();
                },
                commonExecutorService);
    }

    @Override
    public void assign(Collection<MessageQueue> messageQueues) {
        this.consumer.assign(messageQueues);
    }

    @Override
    public Set<MessageQueue> assignment() {
        try {
            return this.consumer.assignment();
        } catch (MQClientException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public List<MessageView> poll(Duration timeout) {
        return this.consumer.poll(timeout.toMillis()).stream()
                .map((Function<MessageExt, MessageView>) MessageViewExt::new)
                .collect(Collectors.toList());
    }

    @Override
    public void wakeup() {
        // wakeup long polling
        try {
            Set<MessageQueue> assignment = this.consumer.assignment();
            if (assignment != null) {
                this.consumer.pause(assignment);
            }
        } catch (MQClientException e) {
            LOG.warn("Consume wakeup long polling failed", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void seek(MessageQueue messageQueue, long offset) {
        try {
            this.consumer.seek(messageQueue, offset);
            LOG.info(
                    "Consumer current offset has been reset, mq={}, next poll will start from offset={}",
                    UtilAll.getQueueDescription(messageQueue),
                    offset);
        } catch (MQClientException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void pause(Collection<MessageQueue> messageQueues) {
        this.consumer.pause(messageQueues);
        LOG.info("Consumer pause fetch messages, mq(s)={}", messageQueues);
    }

    @Override
    public void resume(Collection<MessageQueue> messageQueues) {
        this.consumer.resume(messageQueues);
        LOG.info("Consumer resume fetch messages, mq(s)={}", messageQueues);
    }

    @Override
    public CompletableFuture<Long> seekCommittedOffset(MessageQueue messageQueue) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        long offset =
                                consumer.getOffsetStore()
                                        .readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);

                        if (offset == -1) {
                            offset = minOffset(messageQueue);
                            LOG.info(
                                    "Consumer seek committed offset from remote, offset=-1,mq={},use minOffset={}",
                                    UtilAll.getQueueDescription(messageQueue),
                                    offset);
                        }
                        LOG.info(
                                "Consumer seek committed offset from remote, mq={}, offset={}",
                                UtilAll.getQueueDescription(messageQueue),
                                offset);
                        return offset;
                    } catch (Exception e) {
                        LOG.error(
                                "Consumer seek committed offset from remote error, mq={}",
                                UtilAll.getQueueDescription(messageQueue),
                                e);
                        throw new RuntimeException(e);
                    }
                },
                commonExecutorService);
    }

    @Override
    public CompletableFuture<Long> seekMinOffset(MessageQueue messageQueue) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        long offset = minOffset(messageQueue);
                        LOG.info(
                                "Consumer seek min offset from remote, mq={}, offset={}",
                                UtilAll.getQueueDescription(messageQueue),
                                offset);
                        return offset;
                    } catch (Exception e) {
                        LOG.info(
                                "Consumer seek min offset from remote error, mq={}",
                                UtilAll.getQueueDescription(messageQueue),
                                e);
                        throw new RuntimeException(e);
                    }
                },
                commonExecutorService);
    }

    @Override
    public CompletableFuture<Long> seekMaxOffset(MessageQueue messageQueue) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        long offset = maxOffset(messageQueue);
                        LOG.info(
                                "Consumer seek max offset from remote, mq={}, offset={}",
                                UtilAll.getQueueDescription(messageQueue),
                                offset);
                        return offset;
                    } catch (Exception e) {
                        LOG.info(
                                "Consumer seek max offset from remote error, mq={}",
                                UtilAll.getQueueDescription(messageQueue),
                                e);
                        throw new RuntimeException(e);
                    }
                },
                commonExecutorService);
    }

    /**
     * Query the min offset via the admin client, falling back to a consumer-side timestamp lookup.
     * Aliyun commercial instances expose logical broker names that the admin publish-route lookup
     * cannot resolve, while the consumer request path resolves them through the proxy.
     */
    private long minOffset(MessageQueue messageQueue) throws MQClientException {
        try {
            return adminExt.minOffset(messageQueue);
        } catch (MQClientException e) {
            LOG.info(
                    "Admin min offset lookup failed for mq={}, falling back to consumer lookup",
                    UtilAll.getQueueDescription(messageQueue));
            return consumer.offsetForTimestamp(messageQueue, 0L);
        }
    }

    /** Same admin-first, consumer-fallback strategy as {@link #minOffset(MessageQueue)}. */
    private long maxOffset(MessageQueue messageQueue) throws MQClientException {
        try {
            return adminExt.maxOffset(messageQueue);
        } catch (MQClientException e) {
            LOG.info(
                    "Admin max offset lookup failed for mq={}, falling back to consumer lookup",
                    UtilAll.getQueueDescription(messageQueue));
            return consumer.offsetForTimestamp(
                    messageQueue, System.currentTimeMillis() + TimeUnit.DAYS.toMillis(365));
        }
    }

    @Override
    public CompletableFuture<Long> seekOffsetByTimestamp(
            MessageQueue messageQueue, long timestamp) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        long offset;
                        try {
                            offset = adminExt.searchOffset(messageQueue, timestamp);
                        } catch (MQClientException e) {
                            LOG.info(
                                    "Admin search offset failed for mq={}, falling back to consumer lookup",
                                    UtilAll.getQueueDescription(messageQueue));
                            offset = consumer.offsetForTimestamp(messageQueue, timestamp);
                        }
                        LOG.info(
                                "Consumer seek offset by timestamp from remote, mq={}, timestamp={}, offset={}",
                                UtilAll.getQueueDescription(messageQueue),
                                timestamp,
                                offset);
                        return offset;
                    } catch (MQClientException e) {
                        LOG.info(
                                "Consumer seek offset by timestamp from remote error, mq={}, timestamp={}",
                                UtilAll.getQueueDescription(messageQueue),
                                timestamp,
                                e);
                        throw new RuntimeException(e);
                    }
                },
                commonExecutorService);
    }

    @Override
    public CompletableFuture<Void> commitOffset(MessageQueue messageQueue, long offset) {
        return CompletableFuture.runAsync(
                () -> consumer.getOffsetStore().updateOffset(messageQueue, offset, true),
                commonExecutorService);
    }

    /** The implementation for offsets retriever with a consumer and an admin client. */
    @VisibleForTesting
    public static class RemotingOffsetsRetrieverImpl
            implements OffsetsSelector.MessageQueueOffsetsRetriever, AutoCloseable {

        private final InnerConsumer innerConsumer;

        public RemotingOffsetsRetrieverImpl(InnerConsumer innerConsumer) {
            this.innerConsumer = innerConsumer;
        }

        @Override
        public void close() throws Exception {
            this.innerConsumer.close();
        }

        @Override
        public Map<MessageQueue, Long> committedOffsets(Collection<MessageQueue> messageQueues) {
            return fetchOffsets(
                    messageQueues,
                    mq -> innerConsumer.seekCommittedOffset(mq),
                    "fetch committed offset");
        }

        @Override
        public Map<MessageQueue, Long> minOffsets(Collection<MessageQueue> messageQueues) {
            return fetchOffsets(
                    messageQueues, mq -> innerConsumer.seekMinOffset(mq), "fetch min offset");
        }

        @Override
        public Map<MessageQueue, Long> maxOffsets(Collection<MessageQueue> messageQueues) {
            return fetchOffsets(
                    messageQueues, mq -> innerConsumer.seekMaxOffset(mq), "fetch max offset");
        }

        @Override
        public Map<MessageQueue, Long> offsetsForTimes(
                Map<MessageQueue, Long> messageQueueWithTimeMap) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture<?>[] futures =
                    messageQueueWithTimeMap.entrySet().stream()
                            .map(
                                    entry ->
                                            innerConsumer
                                                    .seekOffsetByTimestamp(
                                                            entry.getKey(), entry.getValue())
                                                    .thenAccept(
                                                            offset ->
                                                                    result.put(
                                                                            entry.getKey(), offset))
                                                    .exceptionally(
                                                            ex -> {
                                                                LOG.error(
                                                                        "Consumer offsets retriever fetch offset by timestamp error",
                                                                        ex);
                                                                return null;
                                                            }))
                            .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(futures).join();
            return result;
        }

        private Map<MessageQueue, Long> fetchOffsets(
                Collection<MessageQueue> messageQueues,
                Function<MessageQueue, CompletableFuture<Long>> offsetFetcher,
                String errorDescription) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture<?>[] futures =
                    messageQueues.stream()
                            .map(
                                    mq ->
                                            offsetFetcher
                                                    .apply(mq)
                                                    .thenAccept(offset -> result.put(mq, offset)))
                            .toArray(CompletableFuture[]::new);
            try {
                CompletableFuture.allOf(futures).join();
            } catch (CompletionException e) {
                throw new FlinkRuntimeException(
                        "Consumer offsets retriever " + errorDescription + " error", e.getCause());
            }
            return result;
        }
    }
}
