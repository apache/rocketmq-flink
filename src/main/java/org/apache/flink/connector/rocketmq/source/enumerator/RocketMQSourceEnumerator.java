/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.source.enumerator;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rocketmq.source.InnerConsumer;
import org.apache.flink.connector.rocketmq.source.InnerConsumerImpl;
import org.apache.flink.connector.rocketmq.source.RocketMQSourceOptions;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategy;
import org.apache.flink.connector.rocketmq.source.enumerator.allocate.AllocateStrategyFactory;
import org.apache.flink.connector.rocketmq.source.enumerator.offset.OffsetsSelector;
import org.apache.flink.connector.rocketmq.source.split.RocketMQSourceSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** The enumerator class for RocketMQ source. */
@Internal
public class RocketMQSourceEnumerator
        implements SplitEnumerator<RocketMQSourceSplit, RocketMQSourceEnumState> {

    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceEnumerator.class);

    private final Configuration configuration;
    private final SplitEnumeratorContext<RocketMQSourceSplit> context;
    private final Boundedness boundedness;

    private InnerConsumer consumer;

    // Users can specify the starting / stopping offset initializer.
    private final AllocateStrategy allocateStrategy;
    private final OffsetsSelector startingOffsetsSelector;
    private final OffsetsSelector stoppingOffsetsSelector;

    // The internal states of the enumerator.
    // This set is only accessed by the partition discovery callable in the callAsync() method.
    // The current assignment by reader id. Only accessed by the coordinator thread.
    // The discovered and initialized partition splits that are waiting for owner reader to be
    // ready.
    private final Set<MessageQueue> allocatedSet;
    private final Map<Integer, Set<RocketMQSourceSplit>> pendingSplitAssignmentMap;

    // Param from configuration
    private final String groupId;
    private final long partitionDiscoveryIntervalMs;

    public RocketMQSourceEnumerator(
            OffsetsSelector startingOffsetsSelector,
            OffsetsSelector stoppingOffsetsSelector,
            Boundedness boundedness,
            Configuration configuration,
            SplitEnumeratorContext<RocketMQSourceSplit> context) {

        this(
                startingOffsetsSelector,
                stoppingOffsetsSelector,
                boundedness,
                configuration,
                context,
                new HashSet<>());
    }

    public RocketMQSourceEnumerator(
            OffsetsSelector startingOffsetsSelector,
            OffsetsSelector stoppingOffsetsSelector,
            Boundedness boundedness,
            Configuration configuration,
            SplitEnumeratorContext<RocketMQSourceSplit> context,
            Set<MessageQueue> currentSplitAssignment) {
        this.configuration = configuration;
        this.context = context;
        this.boundedness = boundedness;

        // Support allocate splits to reader
        this.pendingSplitAssignmentMap = new ConcurrentHashMap<>();
        this.allocatedSet = new HashSet<>(currentSplitAssignment);
        this.allocateStrategy =
                AllocateStrategyFactory.getStrategy(
                        configuration, context, new RocketMQSourceEnumState(allocatedSet));

        // For rocketmq setting
        this.groupId = configuration.getString(RocketMQSourceOptions.CONSUMER_GROUP);
        this.startingOffsetsSelector = startingOffsetsSelector;
        this.stoppingOffsetsSelector = stoppingOffsetsSelector;
        this.partitionDiscoveryIntervalMs =
                configuration.getLong(RocketMQSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
    }

    @Override
    public void start() {
        consumer = new InnerConsumerImpl(configuration);
        consumer.start();

        if (partitionDiscoveryIntervalMs > 0) {
            log.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "with partition discovery interval of {} ms.",
                    groupId,
                    partitionDiscoveryIntervalMs);

            context.callAsync(
                    this::requestServiceDiscovery,
                    this::handleSourceQueueChange,
                    0,
                    partitionDiscoveryIntervalMs);
        } else {
            log.info(
                    "Starting the RocketMQSourceEnumerator for consumer group {} "
                            + "without periodic partition discovery.",
                    groupId);

            context.callAsync(this::requestServiceDiscovery, this::handleSourceQueueChange);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // the rocketmq source pushes splits eagerly, rather than act upon split requests
    }

    /**
     * Add a split back to the split enumerator. It will only happen when a {@link SourceReader}
     * fails and there are splits assigned to it after the last successful checkpoint.
     *
     * @param splits The split to add back to the enumerator for reassignment.
     * @param subtaskId The id of the subtask to which the returned splits belong.
     */
    @Override
    public void addSplitsBack(List<RocketMQSourceSplit> splits, int subtaskId) {
        SourceSplitChangeResult sourceSplitChangeResult =
                new SourceSplitChangeResult(new HashSet<>(splits));
        this.calculateSplitAssignment(sourceSplitChangeResult);
        // If the failed subtask has already restarted, we need to assign splits to it
        if (context.registeredReaders().containsKey(subtaskId)) {
            sendSplitChangesToRemote(Collections.singleton(subtaskId));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        log.debug(
                "Adding reader {} to RocketMQSourceEnumerator for consumer group {}.",
                subtaskId,
                groupId);
        sendSplitChangesToRemote(Collections.singleton(subtaskId));
        if (this.boundedness == Boundedness.BOUNDED) {
            // for RocketMQ bounded source,
            // send this signal to ensure the task can end after all the splits assigned are
            // completed.
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public RocketMQSourceEnumState snapshotState(long checkpointId) {
        return new RocketMQSourceEnumState(allocatedSet);
    }

    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
                consumer = null;
            } catch (Exception e) {
                log.error("Shutdown rocketmq internal consumer error", e);
            }
        }
    }

    // ----------------- private methods -------------------

    private Set<MessageQueue> requestServiceDiscovery() {
        Set<String> topicSet =
                Sets.newHashSet(
                        configuration
                                .getString(RocketMQSourceOptions.TOPIC)
                                .split(RocketMQSourceOptions.TOPIC_SEPARATOR));

        return topicSet.stream()
                .flatMap(
                        topic -> {
                            try {
                                return consumer.fetchMessageQueues(topic).get().stream();
                            } catch (Exception e) {
                                log.error(
                                        "Request topic route for service discovery error, topic={}",
                                        topic,
                                        e);
                            }
                            return Stream.empty();
                        })
                .collect(Collectors.toSet());
    }

    // This method should only be invoked in the coordinator executor thread.
    private void handleSourceQueueChange(Set<MessageQueue> latestSet, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to handle source splits change due to ", t);
        }

        final SourceChangeResult sourceChangeResult = getSourceChangeResult(latestSet);
        if (sourceChangeResult.isEmpty()) {
            log.debug("Skip handle source allocated due to not queue change");
            return;
        }

        context.callAsync(
                () -> initializeSourceSplits(sourceChangeResult), this::handleSplitChanges);
    }

    // This method should only be invoked in the coordinator executor thread.
    private SourceSplitChangeResult initializeSourceSplits(SourceChangeResult sourceChangeResult) {
        Set<MessageQueue> increaseSet = sourceChangeResult.getIncreaseSet();

        OffsetsSelector.MessageQueueOffsetsRetriever offsetsRetriever =
                new InnerConsumerImpl.RemotingOffsetsRetrieverImpl(consumer);

        Map<MessageQueue, Long> startingOffsets =
                startingOffsetsSelector.getMessageQueueOffsets(increaseSet, offsetsRetriever);
        Map<MessageQueue, Long> stoppingOffsets =
                stoppingOffsetsSelector.getMessageQueueOffsets(increaseSet, offsetsRetriever);

        Set<RocketMQSourceSplit> increaseSplitSet =
                increaseSet.stream()
                        .map(
                                mq -> {
                                    long startingOffset = startingOffsets.get(mq);
                                    long stoppingOffset =
                                            stoppingOffsets.getOrDefault(
                                                    mq, RocketMQSourceSplit.NO_STOPPING_OFFSET);
                                    return new RocketMQSourceSplit(
                                            mq, startingOffset, stoppingOffset);
                                })
                        .collect(Collectors.toSet());

        return new SourceSplitChangeResult(increaseSplitSet, sourceChangeResult.getDecreaseSet());
    }

    /**
     * Mark partition splits initialized by {@link
     * RocketMQSourceEnumerator#initializeSourceSplits(SourceChangeResult)} as pending and try to
     * assign pending splits to registered readers.
     *
     * <p>NOTE: This method should only be invoked in the coordinator executor thread.
     *
     * @param sourceSplitChangeResult Partition split changes
     * @param t Exception in worker thread
     */
    private void handleSplitChanges(SourceSplitChangeResult sourceSplitChangeResult, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to initialize partition splits due to ", t);
        }
        if (partitionDiscoveryIntervalMs <= 0) {
            log.info("Split changes, but dynamic partition discovery is disabled.");
        }
        this.calculateSplitAssignment(sourceSplitChangeResult);
        this.sendSplitChangesToRemote(context.registeredReaders().keySet());
    }

    /** Calculate new split assignment according allocate strategy */
    private void calculateSplitAssignment(SourceSplitChangeResult sourceSplitChangeResult) {
        Map<Integer, Set<RocketMQSourceSplit>> newSourceSplitAllocateMap =
                this.allocateStrategy.allocate(
                        sourceSplitChangeResult.getIncreaseSet(), context.currentParallelism());

        for (Map.Entry<Integer, Set<RocketMQSourceSplit>> entry :
                newSourceSplitAllocateMap.entrySet()) {
            this.pendingSplitAssignmentMap
                    .computeIfAbsent(entry.getKey(), r -> new HashSet<>())
                    .addAll(entry.getValue());
        }
    }

    // This method should only be invoked in the coordinator executor thread.
    private void sendSplitChangesToRemote(Set<Integer> pendingReaders) {
        Map<Integer, List<RocketMQSourceSplit>> incrementalSplit = new ConcurrentHashMap<>();

        for (Integer pendingReader : pendingReaders) {
            if (!context.registeredReaders().containsKey(pendingReader)) {
                throw new IllegalStateException(
                        String.format(
                                "Reader %d is not registered to source coordinator",
                                pendingReader));
            }

            final Set<RocketMQSourceSplit> pendingAssignmentForReader =
                    this.pendingSplitAssignmentMap.remove(pendingReader);

            // Put pending assignment into incremental assignment
            if (pendingAssignmentForReader != null && !pendingAssignmentForReader.isEmpty()) {
                incrementalSplit
                        .computeIfAbsent(pendingReader, k -> new ArrayList<>())
                        .addAll(pendingAssignmentForReader);
                pendingAssignmentForReader.forEach(
                        split -> this.allocatedSet.add(split.getMessageQueue()));
            }
        }

        // Assign pending splits to readers
        if (!incrementalSplit.isEmpty()) {
            log.info(
                    "Enumerator assigning split(s) to readers {}",
                    JSON.toJSONString(incrementalSplit, false));
            context.assignSplits(new SplitsAssignment<>(incrementalSplit));
        }

        // Sends NoMoreSplitsEvent to the readers if there is no more partition.
        if (partitionDiscoveryIntervalMs <= 0 && this.boundedness == Boundedness.BOUNDED) {
            log.info(
                    "No more rocketmq partition to assign. "
                            + "Sending NoMoreSplitsEvent to the readers in consumer group {}.",
                    groupId);
            pendingReaders.forEach(this.context::signalNoMoreSplits);
        }
    }

    /** A container class to hold the newly added partitions and removed partitions. */
    @VisibleForTesting
    private static class SourceChangeResult {
        private final Set<MessageQueue> increaseSet;
        private final Set<MessageQueue> decreaseSet;

        public SourceChangeResult(Set<MessageQueue> increaseSet, Set<MessageQueue> decreaseSet) {
            this.increaseSet = increaseSet;
            this.decreaseSet = decreaseSet;
        }

        public Set<MessageQueue> getIncreaseSet() {
            return increaseSet;
        }

        public Set<MessageQueue> getDecreaseSet() {
            return decreaseSet;
        }

        public boolean isEmpty() {
            return increaseSet.isEmpty() && decreaseSet.isEmpty();
        }
    }

    @VisibleForTesting
    public static class SourceSplitChangeResult {

        private final Set<RocketMQSourceSplit> increaseSet;
        private final Set<MessageQueue> decreaseSet;

        private SourceSplitChangeResult(Set<RocketMQSourceSplit> increaseSet) {
            this.increaseSet = Collections.unmodifiableSet(increaseSet);
            this.decreaseSet = Sets.newHashSet();
        }

        private SourceSplitChangeResult(
                Set<RocketMQSourceSplit> increaseSet, Set<MessageQueue> decreaseSet) {
            this.increaseSet = Collections.unmodifiableSet(increaseSet);
            this.decreaseSet = Collections.unmodifiableSet(decreaseSet);
        }

        public Set<RocketMQSourceSplit> getIncreaseSet() {
            return increaseSet;
        }

        public Set<MessageQueue> getDecreaseSet() {
            return decreaseSet;
        }
    }

    @VisibleForTesting
    private SourceChangeResult getSourceChangeResult(Set<MessageQueue> latestSet) {
        Set<MessageQueue> currentSet = Collections.unmodifiableSet(this.allocatedSet);
        Set<MessageQueue> increaseSet = Sets.difference(latestSet, currentSet);
        Set<MessageQueue> decreaseSet = Sets.difference(currentSet, latestSet);

        SourceChangeResult changeResult = new SourceChangeResult(increaseSet, decreaseSet);

        // Current topic route is same as before
        if (changeResult.isEmpty()) {
            log.info(
                    "Request topic route for service discovery, current allocated queues size={}",
                    currentSet.size());
        } else {
            log.info(
                    "Request topic route for service discovery, current allocated queues size: {}. "
                            + "Changed details, current={}, latest={}, increase={}, decrease={}",
                    currentSet.size(),
                    currentSet,
                    latestSet,
                    increaseSet,
                    decreaseSet);
        }
        return changeResult;
    }
}
