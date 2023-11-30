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

import com.alibaba.fastjson.JSON;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.reader.MessageView;
import org.apache.flink.connector.rocketmq.source.reader.MessageViewExt;
import org.apache.flink.connector.rocketmq.source.util.UtilAll;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.StringUtils;
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

        this.adminExt.setNamesrvAddr(endPoints);
        this.adminExt.setAdminExtGroup(groupId);
        this.adminExt.setVipChannelEnabled(false);
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
                            offset = adminExt.minOffset(messageQueue);
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
                        long offset = adminExt.minOffset(messageQueue);
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
                        long offset = adminExt.maxOffset(messageQueue);
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

    @Override
    public CompletableFuture<Long> seekOffsetByTimestamp(
            MessageQueue messageQueue, long timestamp) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        long offset = adminExt.searchOffset(messageQueue, timestamp);
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
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(
                            messageQueues.stream()
                                    .map(
                                            messageQueue ->
                                                    CompletableFuture.supplyAsync(
                                                                    () ->
                                                                            innerConsumer
                                                                                    .seekCommittedOffset(
                                                                                            messageQueue))
                                                            .thenAccept(
                                                                    future -> {
                                                                        try {
                                                                            result.put(
                                                                                    messageQueue,
                                                                                    future.get());
                                                                        } catch (Exception e) {
                                                                            LOG.error(
                                                                                    "Consumer offsets retriever fetch committed offset error",
                                                                                    e);
                                                                        }
                                                                    }))
                                    .toArray(CompletableFuture[]::new))
                    .join();
            return result;
        }

        @Override
        public Map<MessageQueue, Long> minOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(
                            messageQueues.stream()
                                    .map(
                                            messageQueue ->
                                                    CompletableFuture.supplyAsync(
                                                                    () ->
                                                                            innerConsumer
                                                                                    .seekMinOffset(
                                                                                            messageQueue))
                                                            .thenAccept(
                                                                    future -> {
                                                                        try {
                                                                            result.put(
                                                                                    messageQueue,
                                                                                    future.get());
                                                                        } catch (Exception e) {
                                                                            LOG.error(
                                                                                    "Consumer offsets retriever fetch min offset error",
                                                                                    e);
                                                                        }
                                                                    }))
                                    .toArray(CompletableFuture[]::new))
                    .join();
            return result;
        }

        @Override
        public Map<MessageQueue, Long> maxOffsets(Collection<MessageQueue> messageQueues) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(
                            messageQueues.stream()
                                    .map(
                                            messageQueue ->
                                                    CompletableFuture.supplyAsync(
                                                                    () ->
                                                                            innerConsumer
                                                                                    .seekMaxOffset(
                                                                                            messageQueue))
                                                            .thenAccept(
                                                                    future -> {
                                                                        try {
                                                                            result.put(
                                                                                    messageQueue,
                                                                                    future.get());
                                                                        } catch (Exception e) {
                                                                            LOG.error(
                                                                                    "Consumer offsets retriever fetch committed offset error",
                                                                                    e);
                                                                        }
                                                                    }))
                                    .toArray(CompletableFuture[]::new))
                    .join();
            return result;
        }

        @Override
        public Map<MessageQueue, Long> offsetsForTimes(
                Map<MessageQueue, Long> messageQueueWithTimeMap) {
            Map<MessageQueue, Long> result = new ConcurrentHashMap<>();
            CompletableFuture.allOf(
                            messageQueueWithTimeMap.entrySet().stream()
                                    .map(
                                            entry ->
                                                    CompletableFuture.supplyAsync(
                                                                    () ->
                                                                            innerConsumer
                                                                                    .seekOffsetByTimestamp(
                                                                                            entry
                                                                                                    .getKey(),
                                                                                            entry
                                                                                                    .getValue()))
                                                            .thenAccept(
                                                                    future -> {
                                                                        try {
                                                                            result.put(
                                                                                    entry.getKey(),
                                                                                    future.get());
                                                                        } catch (Exception e) {
                                                                            LOG.error(
                                                                                    "Consumer offsets retriever fetch offset by timestamp error",
                                                                                    e);
                                                                        }
                                                                    }))
                                    .toArray(CompletableFuture[]::new))
                    .join();
            return result;
        }
    }
}
